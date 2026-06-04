#!/usr/bin/awk -f
# Diff two GlideFS Prometheus snapshots (before, after) for one export.
# Counters (*_total) show after-before; gauges show before -> after.
# Highlights the substrate-cost metrics first, then dumps the rest.
BEGIN {
  split("glidefs_s3_batches_written_total glidefs_s3_bytes_written_total " \
        "glidefs_s3_bytes_read_total glidefs_s3_read_ops_total " \
        "glidefs_guest_bytes_written_total glidefs_guest_write_ops_total " \
        "glidefs_cache_hits_total glidefs_cache_misses_total " \
        "glidefs_write_amplification glidefs_coalesce_ratio glidefs_dirty_blocks", H, " ")
  for (i in H) hi[H[i]] = i
}
function name(key,   n) { n = key; sub(/[{ ].*/, "", n); return n }
function isctr(key) { return (name(key) ~ /_total$/) }
# parse "metric{labels} value"  -> key=metric{labels}, val=value
function load(file, arr,   line, k, v, p) {
  while ((getline line < file) > 0) {
    if (line ~ /^#/ || line == "") continue
    p = match(line, /[ \t][^ \t]*$/)
    if (!p) continue
    v = substr(line, p+1); gsub(/[ \t]/, "", v)
    k = substr(line, 1, p-1); gsub(/[ \t]+$/, "", k)
    arr[k] = v
  }
  close(file)
}
BEGIN {
  load(ARGV[1], B); load(ARGV[2], A)
  fmt = "%-46s %16s %16s %16s\n"
  printf fmt, "metric (export-scoped)", "before", "after", "delta/Δ"
  printf "%s\n", "----------------------------------------------------------------------------------------------------"
  # highlights first, in declared order
  for (rank=1; rank<=length(H); rank++) {
    for (k in A) {
      if (name(k) != H[rank]) continue
      emit(k)
    }
  }
  printf "%s\n", "---- other export metrics ----"
  for (k in A) { if (!(name(k) in hi)) emit(k) }
}
function emit(k,   b, a, d) {
  b = (k in B) ? B[k] : 0; a = A[k]
  if (isctr(k)) { d = a - b; printf fmt, short(k), b, a, sprintf("%+d", d) }
  else          { printf fmt, short(k), b, a, "(gauge)" }
}
function short(k,   s) { s = k; sub(/^glidefs_/, "", s); sub(/\{export="[^"]*"\}/, "", s);
                        sub(/\{[^}]*export="[^"]*"[^}]*\}/, "", s); return s }
