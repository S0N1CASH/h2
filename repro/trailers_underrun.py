"""h2 #1328：以 trailers 结束流时，content-length 欠发不会被校验。
运行（cwd 任意，依赖仓库自身的 h2/hpack/hyperframe）：
    uv venv --python 3.12 && uv pip install -e . && .venv/bin/python repro/trailers_underrun.py
"""
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.exceptions import ProtocolError
from hpack.hpack import Encoder
from hyperframe.frame import DataFrame, HeadersFrame, SettingsFrame

CONTENT_LENGTH = 15
REQUEST = [
    (":authority", "example.com"),
    (":path", "/"),
    (":scheme", "https"),
    (":method", "POST"),
    ("content-length", str(CONTENT_LENGTH)),
]
TRAILERS = [("x-checksum", "deadbeef")]


def run(body_length, end_with):
    # 每个场景都用全新的 Encoder：HPACK 动态表必须和下面新建的 H2Connection 解码器同步起步，
    # 否则第二个场景会引用对方表里没有的条目。
    encoder = Encoder()

    def headers_frame(items, end_stream=False):
        frame = HeadersFrame(1)
        frame.data = encoder.encode(items)
        frame.flags.add("END_HEADERS")
        if end_stream:
            frame.flags.add("END_STREAM")
        return frame.serialize()

    def data_frame(payload, end_stream=False):
        frame = DataFrame(1)
        frame.data = payload
        frame.flags = {"END_STREAM"} if end_stream else set()
        return frame.serialize()

    settings_frame = SettingsFrame(0)
    settings_frame.settings = {}
    conn = H2Connection(config=H2Configuration(client_side=False))
    conn.initiate_connection()
    conn.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + settings_frame.serialize())
    conn.clear_outbound_data_buffer()
    closing = (
        headers_frame(TRAILERS, end_stream=True)
        if end_with == "trailers"
        else data_frame(b"", end_stream=True)
    )
    try:
        conn.receive_data(headers_frame(REQUEST) + data_frame(b"\x01" * body_length))
    except ProtocolError as error:
        return f"RAISED(在 DATA 阶段) {type(error).__name__}: {error}"
    try:
        events = conn.receive_data(closing)
    except ProtocolError as error:
        return f"RAISED(在 {end_with} 阶段) {type(error).__name__}: {error}"
    return ",".join(type(event).__name__ for event in events)


for body_length in (5, 15, 16):
    for end_with in ("trailers", "empty DATA"):
        expect = "RAISED" if body_length != CONTENT_LENGTH else "no error"
        print(f"body={body_length:>2}/{CONTENT_LENGTH} end={end_with:<10} 期望={expect:<10} 实测: {run(body_length, end_with)}")
