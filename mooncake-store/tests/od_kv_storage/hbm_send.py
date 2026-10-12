import urllib.request, json, os, sys

# HBM 对照实验发请求脚本：同 prompt 发两次（第二次 HBM prefix cache 命中）
# WHY 参数 tag：两次输出分别落盘 /tmp/hbm_out_<tag>.txt，最后 diff 判定
os.environ["no_proxy"] = "127.0.0.1,localhost"
tag = sys.argv[1] if len(sys.argv) > 1 else "run"

# 全新 prompt（与盘框已有的深海 prompt 不同主题，保证第一次是真 O1 全量计算）
PROMPT = (
    "The development of renewable energy technology has accelerated "
    "dramatically over the past two decades. Solar photovoltaic costs have "
    "fallen by nearly ninety percent, making utility scale installations "
    "competitive with fossil fuels in most regions. Wind turbines have grown "
    "larger and more efficient, with modern offshore platforms reaching rotor "
    "diameters exceeding two hundred meters. Energy storage remains the "
    "critical bottleneck, as lithium ion batteries struggle to provide the "
    "long duration capacity needed for multi day weather events. Emerging "
    "alternatives include flow batteries, compressed air systems, and green "
    "hydrogen produced through electrolysis. Grid operators must now balance "
    "traditional baseload generation with variable renewable sources, "
    "requiring sophisticated forecasting algorithms and demand response "
    "programs that incentivize consumers to shift their usage patterns "
    "toward periods of abundant supply."
)

payload = json.dumps({
    "model": "qwen",
    "prompt": PROMPT,
    "max_tokens": 40,
    "temperature": 0,
}).encode()

opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
req = urllib.request.Request(
    "http://127.0.0.1:8000/v1/completions",
    data=payload,
    headers={"Content-Type": "application/json"},
)
with opener.open(req, timeout=300) as r:
    d = json.loads(r.read())

text = d["choices"][0]["text"]
print("=== {} ===".format(tag))
print("OUT: {}".format(repr(text)))
print("TOK: {} -> {}".format(d["usage"]["prompt_tokens"],
                             d["usage"]["completion_tokens"]))
with open("/tmp/hbm_out_{}.txt".format(tag), "w") as f:
    f.write(text)
