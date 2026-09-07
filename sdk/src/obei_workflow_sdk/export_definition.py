"""CLI for exporting a registered workflow as task-system JSON."""

from __future__ import annotations

import argparse
from importlib import import_module
import json
from pathlib import Path
from typing import Any


def _load_runtime(factory_path: str) -> Any:
    """按 ``package.module:callable`` 约定加载宿主 Runtime 工厂。

    只允许显式模块和属性，不扫描项目或执行字符串表达式，既让命令稳定可复现，
    也避免为了便利引入 eval 一类不必要的安全风险。
    """
    try:
        module_name, attribute_name = factory_path.split(":", 1)
    except ValueError as exc:
        raise ValueError("runtime factory must use module:attribute format") from exc
    factory = getattr(import_module(module_name), attribute_name)
    return factory()


def main() -> None:
    """解析命令行、生成定义，并以 UTF-8 输出到文件或标准输出。

    标准输出模式便于与 jq、PowerShell 或 CI 管道组合；文件模式使用缩进和末尾
    换行，方便代码评审。JSON 使用 ensure_ascii=False，中文步骤名保持可读。
    """
    parser = argparse.ArgumentParser(description="Export workflow steps for remote registration")
    parser.add_argument("runtime_factory", help="Runtime factory in module:attribute format")
    parser.add_argument("workflow_type", help="Registered workflow_type")
    parser.add_argument("-o", "--output", help="UTF-8 JSON output path; stdout when omitted")
    args = parser.parse_args()

    # Runtime 工厂仅用于获得 Registry。导出器不会连接数据库、Redis 或模型，
    # 但宿主工厂本身若主动执行 I/O，仍应由宿主保证它适合在 CLI 环境调用。
    runtime = _load_runtime(args.runtime_factory)
    definition = runtime.registry.registration_definition(args.workflow_type)
    content = json.dumps(definition, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        # 明确使用 UTF-8，避免 Windows 默认编码导致中文名称在远端注册时乱码。
        Path(args.output).write_text(content, encoding="utf-8")
    else:
        print(content, end="")


if __name__ == "__main__":
    main()
