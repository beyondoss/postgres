use mio::net::UdpSocket;
use quinn_proto::Transmit;

/// Send a single QUIC `Transmit`. Handles GSO-style segment packing by
/// exploding into individual `send_to` calls (we never enable GSO).
pub fn send_one(socket: &UdpSocket, buf: &[u8], t: &Transmit) {
    if let Some(seg) = t.segment_size {
        let mut off = 0;
        while off < t.size {
            let end = (off + seg).min(t.size);
            let _ = socket.send_to(&buf[off..end], t.destination);
            off = end;
        }
    } else {
        let _ = socket.send_to(&buf[..t.size], t.destination);
    }
}
