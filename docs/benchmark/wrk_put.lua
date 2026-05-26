wrk.headers["Content-Type"] = "application/octet-stream"

local counter = 0

request = function()
  counter = counter + 1
  local key = "bench-key-" .. counter
  local path = "/v0/entity?id=" .. key
  return wrk.format("PUT", path, nil, "benchmark-value")
end

done = function(summary, latency, requests)
  io.write(string.format("custom_p50_ms: %.3f\n", latency:percentile(50.0) / 1000))
  io.write(string.format("custom_p95_ms: %.3f\n", latency:percentile(95.0) / 1000))
  io.write(string.format("custom_p99_ms: %.3f\n", latency:percentile(99.0) / 1000))
  io.write(string.format("custom_rps: %.2f\n", summary.requests / (summary.duration / 1000000)))
end
