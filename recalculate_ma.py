#!/usr/bin/env python3
"""
重新计算所有指数的移动平均线和点位差工具。

使用方法:
    python recalculate_ma.py                    # 重新计算所有指数
    python recalculate_ma.py --index-code H30269  # 只重新计算指定指数
"""

import sys
import argparse
from fund_analyzer.database import SessionLocal, init_db
from fund_analyzer.fetcher import recalculate_moving_averages_and_diffs


def main():
    parser = argparse.ArgumentParser(
        description="重新计算所有指数的移动平均线和点位差"
    )
    parser.add_argument(
        "--index-code",
        type=str,
        default=None,
        help="如果指定，只重新计算该指数的数据；如果不指定，重新计算所有指数",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="显示详细输出",
    )
    
    args = parser.parse_args()
    
    # 初始化数据库
    init_db()
    
    # 创建数据库会话
    db = SessionLocal()
    db.info["operator"] = "recalculate_ma_tool"
    
    try:
        print("=" * 60)
        print("开始重新计算移动平均线和点位差...")
        if args.index_code:
            print(f"目标指数: {args.index_code}")
        else:
            print("目标: 所有指数")
        print("=" * 60)
        
        # 执行重新计算
        updated_count = recalculate_moving_averages_and_diffs(
            db, index_code=args.index_code, verbose=args.verbose
        )
        
        print("=" * 60)
        print(f"重新计算完成！")
        print(f"更新的记录数: {updated_count}")
        print("=" * 60)
        
        return 0
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        import traceback
        if args.verbose:
            traceback.print_exc()
        db.rollback()
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
