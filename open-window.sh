#!/bin/bash
# EzraTube 对外窗口：本地静态服务(8770) + serveo 隧道（幂等，重复跑安全）
# 与教会系统(8765)完全独立，只借用其 proxyconnect.py 过代理的做法。
# 用法：bash open-window.sh → 输出当前公网地址
cd "$(dirname "$0")"
PROXYCONNECT="$HOME/workspace/church-chain-system/proxyconnect.py"

# 1) 本地服务（8770 没监听才启动）
if ! ss -ltn 2>/dev/null | grep -q ':8770 '; then
  (setsid nohup python3 -m http.server 8770 --bind 127.0.0.1 > /tmp/ezratube-server.log 2>&1 &)
  for i in $(seq 1 10); do
    sleep 1
    ss -ltn 2>/dev/null | grep -q ':8770 ' && break
  done
fi

# 2) 隧道（针对 8770 的 serveo 进程已存在就不重开）
if ! pgrep -f "127.0.0.1:8770 serveo.net" >/dev/null 2>&1; then
  rm -f /tmp/ezratube-serveo.log
  (setsid nohup bash -c 'sleep infinity | exec ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ProxyCommand="python3 '"$PROXYCONNECT"' %h %p" -p 443 -R 80:127.0.0.1:8770 serveo.net' > /tmp/ezratube-serveo.log 2>&1 &)
  for i in $(seq 1 20); do
    sleep 1
    grep -qo "https://[a-z0-9-]*\.serveousercontent\.com" /tmp/ezratube-serveo.log 2>/dev/null && break
  done
fi

grep -o "https://[a-z0-9-]*\.serveousercontent\.com" /tmp/ezratube-serveo.log 2>/dev/null | head -1
