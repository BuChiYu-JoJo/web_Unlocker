import csv
import json
import math
import random
import threading
import time
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests

# ================== 配置区域 ==================
# 需要随机请求的 URL 列表（无需命令行传入）
URLS = [
    "https://google.com",
    "https://bing.com",
    "https://duckduckgo.com",
    "https://yandex.com",
    "https://pedigreequery.com",
    "https://tmeviral.site",
    "https://senado.gob.mx",
    "https://investing.com",
    "https://tbankrot.ru",
    "https://infosen.senado.gob.mx",
    "https://produto.mercadolivre.com.br",
    "https://prompthero.com",
    "https://reddit.com",
    "https://mercadolivre.com.br",
    "https://yandex.ru",
    "https://yandex.cloud",
    "https://thehackernews.com",
    "https://freepik.com",
    "https://pneustore.com.br",
    "https://acheipneus.com.br",
    "https://t.me",
    "https://apps.apple.com",
    "https://pneufree.com.br",
    "https://play.google.com",
    "https://duck.ai",
    "https://shutterstock.com",
    "https://bingapp.microsoft.com",
    "https://vc.ru",
    "https://tenchat.ru",
    "https://varellamotos.com.br",
    "https://dzen.ru",
    "https://seopage.ai",
    "https://amazon.com",
    "https://autoscout24.be",
    "https://data.similarweb.com",
    "https://tg-cat.com",
    "https://browserdefaults.microsoft.com",
    "https://browserdefaults.microsoft.com",
    "https://cults3d.com",
    "https://telegram.im",
    "https://cgtrader.com",
    "https://consultant.ru",
    "https://etsy.com",
    "https://printables.com",
    "https://tiktok.com",
    "https://zara.com",
    "https://telemetr.io",
    "https://httpbin.org",
    "https://redfin.com",
    "https://2dehands.be",
    "https://telegram.me",
    "https://hipervarejo.com.br",
    "https://linkedin.com",
    "https://shopee.co.th",
    "https://rutube.ru",
    "https://tbank.ru",
    "https://shopee.co.th",
    "https://realtor.com",
    "https://edge.allegro.pl",
    "https://martinensepneus.com.br",
    "https://clickup.com",
    "https://sbersova.ru",
    "https://sephora.com",
    "https://us.puma.com",
    "https://uslugi.yandex.ru",
    "https://vrbo.com",
    "https://github.com",
    "https://ping0.cc",
    "https://2gis.ru",
    "https://twitch.tv",
    "https://ozon.ru",
    "https://velocepneus.com",
    "https://hopee.com.br",
    "https://zoon.ru",
    "https://klerk.ru",
    "https://ping0.cc",
    "https://news.aibase.cn",
    "https://uslugio.com",
    "https://spb.specbusiness.ru",
    "https://academy-health.ru",
    "https://yohomobile.com",
    "https://allfinancelinks.com",
    "https://seenoai.com",
    "https://bcs-express.ru",
    "https://drivepneus.com.br",
    "https://journal.sovcombank.ru",
    "https://pikabu.ru",
    "https://Avito.ru",
    "https://finadvisor.info",
    "https://smart-lab.ru",
    "https://teletype.in",
    "https://maint.broward.org",
    "https://cryptorussia.ru",
    "https://atacadaopneus.com.br",
    "https://instagram.com",
    "https://saksfifthavenue.com",
    "https://ok.ru",
]

# 并发数量（默认值，可通过命令行参数覆盖，支持一次传多个并发依次执行）
CONCURRENCY_LIST = [5]

# 每个并发配置下的请求总数（可通过命令行参数覆盖）
TOTAL_REQUESTS = 500

# 每个 URL 的请求次数（可通过命令行参数覆盖）
REQUESTS_PER_URL = 10

# 结果输出目录
OUTPUT_DIR = Path("brightdata_results")

# Bright Data Web Unlocker 接口配置
BASE_URL = "https://api.brightdata.com/request"
AUTHORIZATION = "Bearer 6c62b12c481a172f51629ed915ae0bd7f43eff9794aa391505bc354b154f339a"
ZONE = "web_unlocker2"
# =============================================


def do_request(url: str) -> Dict[str, Any]:
    start = time.perf_counter()
    started_at = datetime.now().isoformat()
    http_status = None
    http_reason = None
    body_text = ""

    payload = {
        "zone": ZONE,
        "url": url,
        "format": "json",
        "method": "GET",
        "direct": False,
    }

    try:
        response = requests.post(
            BASE_URL,
            json=payload,
            headers={
                "Authorization": AUTHORIZATION,
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        http_status = response.status_code
        http_reason = response.reason
        body_text = response.text
    except Exception as exc:
        duration = time.perf_counter() - start
        return {
            "请求时间": started_at,
            "请求URL": url,
            "响应时间(s)": round(duration, 4),
            "接口状态码": http_status if http_status is not None else "n/a",
            "响应内容状态码": "n/a",
            "是否成功": False,
            "接口错误码": http_status if http_status is not None else "n/a",
            "响应内容错误码": "",
            "错误备注": f"Interface error: {exc}",
            "响应文件大小(KB)": round(len(body_text.encode("utf-8")) / 1024, 4)
            if body_text
            else 0.0,
        }

    duration = time.perf_counter() - start
    payload_status: Any = "n/a"
    payload_error_code: Any = ""
    error_notes = ""
    success = False
    response_time = duration

    try:
        payload_json = json.loads(body_text) if body_text else {}
        payload_status = payload_json.get("status_code", "n/a")
        payload_error_code = payload_status if payload_status != 200 else ""
        body_content = payload_json.get("body")
        response_size_kb = len(body_text.encode("utf-8")) / 1024 if body_text else 0.0

        response_time_field = payload_json.get("response_time")
        if response_time_field is not None:
            try:
                response_time = float(response_time_field)
            except (TypeError, ValueError):
                response_time = duration

        if (
            http_status == 200
            and payload_status == 200
            and body_content
            and response_size_kb > 2.0
        ):
            success = True
        else:
            if http_status != 200:
                error_notes = f"Interface error: {http_reason}"
            elif payload_status != 200:
                error_detail = payload_json.get("error")
                if error_detail is None:
                    error_detail = payload_json
                error_notes = f"Payload error: {error_detail}"
            elif not body_content:
                payload_error_code = "empty_body"
                error_notes = "Payload error: body is empty"
            elif response_size_kb <= 2.0:
                payload_error_code = "response_too_small"
                error_notes = "Payload error: response size <= 2KB"
    except Exception as exc:
        payload_status = "invalid_json"
        payload_error_code = "invalid_json"
        error_notes = f"Response parse error: {exc}"

    return {
        "请求时间": started_at,
        "请求URL": url,
        "响应时间(s)": round(response_time, 4),
        "接口状态码": http_status,
        "响应内容状态码": payload_status,
        "是否成功": success,
        "接口错误码": "" if (success or http_status == 200) else http_status,
        "响应内容错误码": "" if success else payload_error_code,
        "错误备注": error_notes,
        "响应文件大小(KB)": round(len(body_text.encode("utf-8")) / 1024, 4),
    }


def percentile(values: List[float], pct: int) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = math.ceil(len(sorted_values) * pct / 100) - 1
    idx = max(0, min(idx, len(sorted_values) - 1))
    return round(sorted_values[idx], 4)


def save_csv(path: Path, rows: List[Dict[str, Any]], headers: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def run(concurrency: int, per_url: int, urls: List[str]) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    lock = threading.Lock()
    request_urls = [url for url in urls for _ in range(per_url)]
    random.shuffle(request_urls)

    def worker_task(request_url: str) -> None:
        result = do_request(request_url)
        with lock:
            results.append(result)

    exec_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(worker_task, request_url) for request_url in request_urls
        ]
        for future in as_completed(futures):
            future.result()
    exec_end = time.perf_counter()
    exec_duration = exec_end - exec_start

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    detail_path = OUTPUT_DIR / f"request_details_c{concurrency}_{timestamp}.csv"
    save_csv(
        detail_path,
        results,
        headers=[
            "请求时间",
            "请求URL",
            "响应时间(s)",
            "接口状态码",
            "响应内容状态码",
            "是否成功",
            "接口错误码",
            "响应内容错误码",
            "错误备注",
            "响应文件大小(KB)",
        ],
    )

    success_rows = [row for row in results if row.get("是否成功")]
    success_times = [row["响应时间(s)"] for row in success_rows]
    success_sizes = [row["响应文件大小(KB)"] for row in success_rows]

    total = len(results)
    success_count = len(success_rows)
    error_count = total - success_count
    success_rate = (success_count / total) if total else 0.0
    error_rate = 1 - success_rate if total else 0.0
    qps = (total / exec_duration) if exec_duration > 0 else 0.0

    stats_row = {
        "并发数": concurrency,
        "QPS": round(qps, 4),
        "请求总数": total,
        "成功请求数": success_count,
        "错误请求数": error_count,
        "成功率": round(success_rate, 4),
        "错误率": round(error_rate, 4),
        "成功平均响应时间(s)": round(sum(success_times) / success_count, 4)
        if success_times
        else 0.0,
        "成功平均响应大小(KB)": round(sum(success_sizes) / success_count, 4)
        if success_sizes
        else 0.0,
        "P50响应时间(s)": percentile(success_times, 50),
        "P75响应时间(s)": percentile(success_times, 75),
        "P90响应时间(s)": percentile(success_times, 90),
        "P95响应时间(s)": percentile(success_times, 95),
        "P99响应时间(s)": percentile(success_times, 99),
        "并发完成时间(s)": round(exec_duration, 4),
    }

    stats_path = OUTPUT_DIR / f"request_stats_c{concurrency}_{timestamp}.csv"
    save_csv(
        stats_path,
        [stats_row],
        headers=[
            "并发数",
            "QPS",
            "请求总数",
            "成功请求数",
            "错误请求数",
            "成功率",
            "错误率",
            "成功平均响应时间(s)",
            "成功平均响应大小(KB)",
            "P50响应时间(s)",
            "P75响应时间(s)",
            "P90响应时间(s)",
            "P95响应时间(s)",
            "P99响应时间(s)",
            "并发完成时间(s)",
        ],
    )

    print(f"并发 {concurrency} 完成: 详细请求结果 -> {detail_path}")
    print(f"并发 {concurrency} 完成: 统计结果 -> {stats_path}")

    return {"detail": detail_path, "stats": stats_path, "stats_row": stats_row}


if __name__ == "__main__":
    parser = ArgumentParser(description="Bright Data Web Unlocker 并发请求脚本")
    parser.add_argument(
        "--concurrency",
        nargs="+",
        type=int,
        help="一个或多个并发值，依次执行（例如: --concurrency 5 10 20）",
    )
    parser.add_argument(
        "--per-url",
        type=int,
        default=REQUESTS_PER_URL,
        help="每个 URL 的请求次数",
    )
    parser.add_argument(
        "--requests",
        type=int,
        help="兼容旧参数：等同于 --per-url",
    )
    args = parser.parse_args()

    concurrency_values: List[int] = (
        list(args.concurrency) if args.concurrency else list(CONCURRENCY_LIST)
    )
    per_url = args.requests if args.requests is not None else args.per_url
    summary_timestamp = time.strftime("%Y%m%d_%H%M%S")
    summary_rows: List[Dict[str, Any]] = []

    for c in concurrency_values:
        result = run(c, per_url, URLS)
        summary_rows.append(result["stats_row"])

    if len(summary_rows) > 1:
        summary_path = OUTPUT_DIR / f"request_stats_summary_{summary_timestamp}.csv"
        save_csv(
            summary_path,
            summary_rows,
            headers=[
                "并发数",
                "QPS",
                "请求总数",
                "成功请求数",
                "错误请求数",
                "成功率",
                "错误率",
                "成功平均响应时间(s)",
                "成功平均响应大小(KB)",
                "P50响应时间(s)",
                "P75响应时间(s)",
                "P90响应时间(s)",
                "P95响应时间(s)",
                "P99响应时间(s)",
                "并发完成时间(s)",
            ],
        )
        print(f"汇总统计结果 -> {summary_path}")
