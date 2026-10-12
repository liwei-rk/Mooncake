import urllib.request, json, os, sys

# 第四次请求（全新天文 prompt）：触发首段 D2H 的延迟复读验证
os.environ["no_proxy"] = "127.0.0.1,localhost"
tag = sys.argv[1] if len(sys.argv) > 1 else "run"

PROMPT = (
    "Astronomers have made remarkable progress in understanding the formation "
    "and evolution of galaxies across cosmic time. The James Webb Space "
    "Telescope, launched in December twenty twenty-one, has observed galaxies "
    "that formed less than one billion years after the Big Bang, challenging "
    "existing models of early cosmic structure. Supermassive black holes, "
    "millions to billions of times the mass of the sun, appear to co-evolve "
    "with their host galaxies through feedback mechanisms that regulate star "
    "formation. Dark matter, comprising roughly eighty five percent of all "
    "matter in the universe, reveals its presence only through gravitational "
    "effects on visible matter and background radiation. Gravitational wave "
    "detectors have now observed dozens of black hole and neutron star "
    "mergers, opening an entirely new window onto the most violent events in "
    "the cosmos and confirming predictions made a century ago by Einstein."
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
