//! pgoutput binary decoder. Stateful: caches RELATION descriptors so subsequent
//! INSERT/UPDATE/DELETE messages can resolve column names and types.

use std::collections::HashMap;

use crate::lsn::Lsn;

// Postgres OIDs we render as JSON-native types instead of strings.
const OID_BOOL: u32 = 16;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_INT8: u32 = 20;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_NUMERIC: u32 = 1700;

pub struct Column {
    pub name: String,
    pub type_oid: u32,
}

pub struct RelationInfo {
    pub schema: String,
    pub table: String,
    pub columns: Vec<Column>,
}

pub struct Decoder {
    relations: HashMap<u32, RelationInfo>,
}

impl Decoder {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
        }
    }

    /// Decode one pgoutput message. Returns Some(json_bytes) for DML events, None
    /// for cache-only messages (relation/begin/commit/truncate/origin/type).
    pub fn decode(&mut self, lsn: Lsn, data: &[u8]) -> Option<Vec<u8>> {
        if data.is_empty() {
            return None;
        }
        let tag = data[0];
        let body = &data[1..];
        let mut p = Parser::new(body);

        match tag {
            b'R' => {
                // RELATION: int32 oid + cstr ns + cstr name + int8 ri + int16 ncols + cols...
                let oid = p.u32()?;
                let schema = p.cstr()?;
                let table = p.cstr()?;
                p.u8()?; // replica identity
                let ncols = p.u16()? as usize;
                let mut columns = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    p.u8()?; // flags
                    let name = p.cstr()?;
                    let type_oid = p.u32()?;
                    p.u32()?; // type modifier
                    columns.push(Column { name, type_oid });
                }
                self.relations.insert(
                    oid,
                    RelationInfo {
                        schema,
                        table,
                        columns,
                    },
                );
                None
            }
            // BEGIN and COMMIT carry no per-row data we need; pgoutput provides
            // per-event LSN on each DML message directly.
            b'B' | b'C' => None,
            b'I' => {
                let rel_oid = p.u32()?;
                p.u8()?; // 'N'
                let new_tuple = read_tuple(&mut p)?;
                let rel = self.relations.get(&rel_oid)?;
                emit_dml(lsn, "insert", rel, None, Some(&new_tuple))
            }
            b'U' => {
                let rel_oid = p.u32()?;
                let mut old_tuple: Option<Vec<TupleVal>> = None;
                let mut marker = p.u8()?;
                if marker == b'O' || marker == b'K' {
                    old_tuple = Some(read_tuple(&mut p)?);
                    marker = p.u8()?;
                }
                if marker != b'N' {
                    return None;
                }
                let new_tuple = read_tuple(&mut p)?;
                let rel = self.relations.get(&rel_oid)?;
                emit_dml(lsn, "update", rel, old_tuple.as_deref(), Some(&new_tuple))
            }
            b'D' => {
                let rel_oid = p.u32()?;
                let marker = p.u8()?;
                if marker != b'O' && marker != b'K' {
                    return None;
                }
                let old_tuple = read_tuple(&mut p)?;
                let rel = self.relations.get(&rel_oid)?;
                emit_dml(lsn, "delete", rel, Some(&old_tuple), None)
            }
            // 'T' (truncate), 'Y' (type), 'O' (origin), 'M' (message) — cache-only / unsupported
            _ => None,
        }
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
enum TupleVal {
    Null,
    Unchanged,
    Text(String),
}

struct Parser<'a> {
    buf: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, i: 0 }
    }

    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.i + n > self.buf.len() {
            return None;
        }
        let s = &self.buf[self.i..self.i + n];
        self.i += n;
        Some(s)
    }

    fn u8(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.i)?;
        self.i += 1;
        Some(b)
    }

    fn u16(&mut self) -> Option<u16> {
        let b = self.bytes(2)?;
        Some(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Option<u32> {
        let b = self.bytes(4)?;
        Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i32(&mut self) -> Option<i32> {
        self.u32().map(|v| v as i32)
    }

    fn cstr(&mut self) -> Option<String> {
        let start = self.i;
        while self.i < self.buf.len() && self.buf[self.i] != 0 {
            self.i += 1;
        }
        if self.i >= self.buf.len() {
            return None;
        }
        let s = String::from_utf8(self.buf[start..self.i].to_vec()).ok()?;
        self.i += 1; // skip NUL
        Some(s)
    }
}

fn read_tuple(p: &mut Parser<'_>) -> Option<Vec<TupleVal>> {
    let n = p.u16()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        match p.u8()? {
            b'n' => out.push(TupleVal::Null),
            b'u' => out.push(TupleVal::Unchanged),
            b't' => {
                let len = p.i32()?;
                if len < 0 {
                    return None;
                }
                let bytes = p.bytes(len as usize)?;
                out.push(TupleVal::Text(String::from_utf8(bytes.to_vec()).ok()?));
            }
            _ => return None,
        }
    }
    Some(out)
}

// --- inline JSON writer ----------------------------------------------------
// Avoids serde_json Map/Value allocations per event. Column names and schema/
// table strings are written directly into the output buffer with proper escaping.

fn write_json_str(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    for b in s.bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x00..=0x1f => {
                out.extend_from_slice(b"\\u00");
                let hi = b >> 4;
                let lo = b & 0xf;
                out.push(if hi < 10 { b'0' + hi } else { b'a' + hi - 10 });
                out.push(if lo < 10 { b'0' + lo } else { b'a' + lo - 10 });
            }
            other => out.push(other),
        }
    }
    out.push(b'"');
}

fn write_json_value(out: &mut Vec<u8>, type_oid: u32, s: &str) {
    match type_oid {
        OID_BOOL => match s {
            "t" => out.extend_from_slice(b"true"),
            "f" => out.extend_from_slice(b"false"),
            _ => write_json_str(out, s),
        },
        OID_INT2 | OID_INT4 | OID_INT8 => {
            if s.parse::<i64>().is_ok() {
                out.extend_from_slice(s.as_bytes());
            } else {
                write_json_str(out, s);
            }
        }
        OID_FLOAT4 | OID_FLOAT8 => {
            match s.parse::<f64>() {
                Ok(f) if f.is_finite() => {
                    // Use serde_json's number formatter for correct representation.
                    let n = serde_json::Number::from_f64(f).unwrap();
                    out.extend_from_slice(n.to_string().as_bytes());
                }
                _ => write_json_str(out, s),
            }
        }
        // NUMERIC is exact-decimal; emit as a JSON string to preserve precision.
        // Parsing via f64 would silently corrupt values like 99.9999999999999.
        OID_NUMERIC => write_json_str(out, s),
        _ => write_json_str(out, s),
    }
}

fn write_tuple_json(out: &mut Vec<u8>, rel: &RelationInfo, tuple: &[TupleVal]) -> bool {
    if tuple.len() != rel.columns.len() {
        eprintln!(
            "error: {}.{} column count mismatch: {} cols in schema, {} in tuple; dropping event",
            rel.schema,
            rel.table,
            rel.columns.len(),
            tuple.len()
        );
        return false;
    }
    out.push(b'{');
    let mut first = true;
    for (col, val) in rel.columns.iter().zip(tuple.iter()) {
        match val {
            TupleVal::Unchanged => continue, // omit TOAST sentinels
            TupleVal::Null => {
                if !first {
                    out.push(b',');
                }
                first = false;
                write_json_str(out, &col.name);
                out.push(b':');
                out.extend_from_slice(b"null");
            }
            TupleVal::Text(s) => {
                if !first {
                    out.push(b',');
                }
                first = false;
                write_json_str(out, &col.name);
                out.push(b':');
                write_json_value(out, col.type_oid, s);
            }
        }
    }
    out.push(b'}');
    true
}

fn emit_dml(
    lsn: Lsn,
    op: &str,
    rel: &RelationInfo,
    old: Option<&[TupleVal]>,
    new: Option<&[TupleVal]>,
) -> Option<Vec<u8>> {
    let lsn_str = lsn.to_string();
    let mut out = Vec::with_capacity(256);
    out.push(b'{');
    write_json_str(&mut out, "lsn");
    out.push(b':');
    write_json_str(&mut out, &lsn_str);
    out.extend_from_slice(b",");
    write_json_str(&mut out, "op");
    out.push(b':');
    write_json_str(&mut out, op);
    out.extend_from_slice(b",");
    write_json_str(&mut out, "schema");
    out.push(b':');
    write_json_str(&mut out, &rel.schema);
    out.extend_from_slice(b",");
    write_json_str(&mut out, "table");
    out.push(b':');
    write_json_str(&mut out, &rel.table);
    if let Some(t) = old {
        out.extend_from_slice(b",\"old\":");
        if !write_tuple_json(&mut out, rel, t) {
            return None;
        }
    }
    if let Some(t) = new {
        out.extend_from_slice(b",\"new\":");
        if !write_tuple_json(&mut out, rel, t) {
            return None;
        }
    }
    out.push(b'}');
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn build_relation_msg(oid: u32, schema: &str, table: &str, cols: &[(&str, u32)]) -> Vec<u8> {
        let mut m = vec![b'R'];
        m.extend_from_slice(&oid.to_be_bytes());
        m.extend_from_slice(schema.as_bytes());
        m.push(0);
        m.extend_from_slice(table.as_bytes());
        m.push(0);
        m.push(b'd'); // replica identity = default
        m.extend_from_slice(&(cols.len() as u16).to_be_bytes());
        for (name, oid) in cols {
            m.push(0); // flags
            m.extend_from_slice(name.as_bytes());
            m.push(0);
            m.extend_from_slice(&oid.to_be_bytes());
            m.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        }
        m
    }

    fn build_insert(rel_oid: u32, vals: &[Option<&str>]) -> Vec<u8> {
        let mut m = vec![b'I'];
        m.extend_from_slice(&rel_oid.to_be_bytes());
        m.push(b'N');
        m.extend_from_slice(&(vals.len() as u16).to_be_bytes());
        for v in vals {
            match v {
                None => m.push(b'n'),
                Some(s) => {
                    m.push(b't');
                    m.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    m.extend_from_slice(s.as_bytes());
                }
            }
        }
        m
    }

    #[test]
    fn insert_emits_typed_json() {
        let mut d = Decoder::new();
        let rel = build_relation_msg(
            42,
            "public",
            "users",
            &[("id", OID_INT4), ("name", 25), ("active", OID_BOOL)],
        );
        assert!(d.decode(Lsn(0x1_0000_0000), &rel).is_none());
        let ins = build_insert(42, &[Some("7"), Some("alice"), Some("t")]);
        let out = d.decode(Lsn(0x1_2345_6780), &ins).expect("dml");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["op"], "insert");
        assert_eq!(v["schema"], "public");
        assert_eq!(v["table"], "users");
        assert_eq!(v["lsn"], "1/23456780");
        assert_eq!(v["new"]["id"], 7);
        assert_eq!(v["new"]["name"], "alice");
        assert_eq!(v["new"]["active"], true);
    }

    fn build_update(
        rel_oid: u32,
        old_marker: u8,
        old_vals: &[Option<&str>],
        old_unchanged: &[bool],
        new_vals: &[Option<&str>],
    ) -> Vec<u8> {
        let mut m = vec![b'U'];
        m.extend_from_slice(&rel_oid.to_be_bytes());
        m.push(old_marker);
        m.extend_from_slice(&(old_vals.len() as u16).to_be_bytes());
        for (i, v) in old_vals.iter().enumerate() {
            if old_unchanged.get(i).copied().unwrap_or(false) {
                m.push(b'u');
                continue;
            }
            match v {
                None => m.push(b'n'),
                Some(s) => {
                    m.push(b't');
                    m.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    m.extend_from_slice(s.as_bytes());
                }
            }
        }
        m.push(b'N');
        m.extend_from_slice(&(new_vals.len() as u16).to_be_bytes());
        for v in new_vals {
            match v {
                None => m.push(b'n'),
                Some(s) => {
                    m.push(b't');
                    m.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    m.extend_from_slice(s.as_bytes());
                }
            }
        }
        m
    }

    #[test]
    fn update_with_replica_identity_full_includes_old_tuple() {
        let mut d = Decoder::new();
        let rel = build_relation_msg(10, "public", "t", &[("id", OID_INT4), ("val", 25)]);
        d.decode(Lsn::ZERO, &rel);
        let upd = build_update(
            10,
            b'O',
            &[Some("7"), Some("before")],
            &[false, false],
            &[Some("7"), Some("after")],
        );
        let out = d.decode(Lsn(1), &upd).expect("dml");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["op"], "update");
        assert_eq!(v["old"]["id"], 7);
        assert_eq!(v["old"]["val"], "before");
        assert_eq!(v["new"]["val"], "after");
    }

    #[test]
    fn toast_unchanged_omitted_from_update() {
        let mut d = Decoder::new();
        let rel = build_relation_msg(11, "public", "t", &[("id", OID_INT4), ("body", 25)]);
        d.decode(Lsn::ZERO, &rel);
        let upd = build_update(
            11,
            b'K',
            &[Some("3"), Some("ignored")],
            &[false, true],
            &[Some("3"), Some("new body")],
        );
        let out = d.decode(Lsn(1), &upd).expect("dml");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["old"]["id"], 3);
        assert!(v["old"].get("body").is_none());
        assert_eq!(v["new"]["body"], "new body");
    }

    #[test]
    fn bool_float_numeric_type_coercion() {
        let mut d = Decoder::new();
        let rel = build_relation_msg(
            12,
            "public",
            "t",
            &[
                ("flag", OID_BOOL),
                ("score", OID_FLOAT8),
                ("amount", OID_NUMERIC),
                ("small", OID_FLOAT4),
                ("count", OID_INT2),
            ],
        );
        d.decode(Lsn::ZERO, &rel);
        let ins = build_insert(
            12,
            &[
                Some("t"),
                Some("3.14"),
                Some("99.99"),
                Some("1.5"),
                Some("42"),
            ],
        );
        let out = d.decode(Lsn(1), &ins).expect("dml");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["new"]["flag"], true);
        assert!(v["new"]["score"].is_f64());
        assert!((v["new"]["score"].as_f64().unwrap() - 3.14).abs() < 1e-9);
        // NUMERIC is emitted as a JSON string to preserve exact decimal precision.
        assert_eq!(v["new"]["amount"], "99.99");
        assert!(v["new"]["small"].is_f64());
        assert_eq!(v["new"]["count"], 42);
    }

    #[test]
    fn null_value_encoded_as_json_null() {
        let mut d = Decoder::new();
        let rel = build_relation_msg(13, "public", "t", &[("id", OID_INT4), ("val", 25)]);
        d.decode(Lsn::ZERO, &rel);
        let ins = build_insert(13, &[Some("1"), None]);
        let out = d.decode(Lsn(1), &ins).expect("dml");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["new"]["id"], 1);
        assert!(v["new"]["val"].is_null());
    }

    #[test]
    fn delete_includes_old_only() {
        let mut d = Decoder::new();
        let rel = build_relation_msg(7, "public", "t", &[("id", OID_INT4)]);
        d.decode(Lsn::ZERO, &rel);
        let mut m = vec![b'D'];
        m.extend_from_slice(&7u32.to_be_bytes());
        m.push(b'O');
        m.extend_from_slice(&1u16.to_be_bytes());
        m.push(b't');
        m.extend_from_slice(&1i32.to_be_bytes());
        m.push(b'9');
        let out = d.decode(Lsn(1), &m).expect("dml");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["op"], "delete");
        assert_eq!(v["old"]["id"], 9);
        assert!(v.get("new").is_none());
    }

    #[test]
    fn json_string_escaping() {
        let mut out = Vec::new();
        write_json_str(&mut out, "tab\there\nnewline\\back\"quote");
        let s = std::str::from_utf8(&out).unwrap();
        // Should round-trip through serde_json
        let parsed: Value = serde_json::from_str(s).unwrap();
        assert_eq!(parsed.as_str().unwrap(), "tab\there\nnewline\\back\"quote");
    }

    #[test]
    fn begin_commit_return_none() {
        let mut d = Decoder::new();
        // BEGIN: int64 final_lsn + int64 ts + int32 xid
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&[0u8; 20]);
        assert!(d.decode(Lsn::ZERO, &begin).is_none());
        // COMMIT: similar layout
        let commit = vec![b'C', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(d.decode(Lsn::ZERO, &commit).is_none());
    }
}
