import glob
import os
import re
import logging
import time
from pathlib import Path
from typing import List, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import psutil

# --------------------------
# 配置模块级日志
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --------------------------
# 常量定义
# --------------------------
MAIN_FOLDER = Path("/data/main_folder")  # 主路径硬编码
REGION = "region"                        # 固定区域名称
SAFE_PATH_PATTERN = re.compile(r'^[\w\-\*\.]+$')  # 允许通配符的安全路径校验

# --------------------------
# 核心处理类（修复版）
# --------------------------
class CSVProcessor:
    def __init__(self, column_types: Dict[str, pa.DataType], required_cols: List[str], max_workers: int = None):
        self.column_types = column_types
        self.required_cols = set(required_cols)
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) * 4
        self._validate_types()

    def _validate_types(self):
        """校验列类型定义的合法性"""
        for col, dtype in self.column_types.items():
            if not isinstance(dtype, pa.DataType):
                raise TypeError(f"Column {col} type must be pyarrow.DataType")

    @staticmethod
    def _is_safe_pattern(pattern: str) -> bool:
        """允许包含通配符的安全校验"""
        return SAFE_PATH_PATTERN.match(pattern) is not None

    def _expand_patterns(self, base_dir: Path, patterns: List[str]) -> List[Path]:
        """安全展开路径模式"""
        expanded = []
        for pattern in patterns:
            if not self._is_safe_pattern(pattern):
                logger.warning(f"跳过非法模式: {pattern}")
                continue
            
            # 转换为绝对路径避免路径遍历
            full_pattern = str(base_dir / pattern)
            matches = glob.glob(full_pattern)
            
            for match in matches:
                path = Path(match)
                if path.is_dir():
                    expanded.append(path.resolve())  # 标准化路径
        return expanded

    def find_files(self, versions: List[str], dates: List[str]) -> List[Path]:
        """支持通配符的路径展开"""
        valid_files = []
        
        # 展开版本目录
        version_dirs = self._expand_patterns(MAIN_FOLDER, versions)
        if not version_dirs:
            logger.warning("未找到匹配的版本目录")
            return []
        
        # 展开日期目录
        for version_dir in version_dirs:
            date_dirs = self._expand_patterns(version_dir, dates)
            for date_dir in date_dirs:
                region_dir = date_dir / REGION
                if region_dir.exists():
                    valid_files.extend(region_dir.glob("*.csv"))
        
        return valid_files

    def _fast_column_check(self, file_path: Path) -> bool:
        """快速列存在性检查（修复空文件处理）"""
        try:
            with open(file_path, 'rb') as f:
                # 仅读取前1行元数据
                table = pa_csv.read_csv(
                    f,
                    read_options=pa_csv.ReadOptions(
                        skip_rows=1,
                        autogenerate_column_names=False
                    )
                )
                return self.required_cols.issubset(table.schema.names)
        except pa.ArrowInvalid as e:
            logger.debug(f"空文件或格式错误 {file_path}: {e}")
            return False
        except Exception as e:
            logger.debug(f"列检查失败 {file_path}: {e}")
            return False

    # 保持其他方法不变...

# --------------------------
# 测试用例
# --------------------------
if __name__ == "__main__":
    # 列类型定义
    column_types = {
        "user_id": pa.string(),
        "price": pa.float32()
    }
    
    # 初始化处理器
    processor = CSVProcessor(
        column_types=column_types,
        required_cols=["user_id", "price"],
        max_workers=8
    )
    
    # 测试通配符
    df = processor.process(
        versions=["*"],    # 匹配所有版本
        dates=["2023-01-*"],  # 匹配所有1月日期
        batch_size=200
    )
    
    print(f"加载到 {len(df)} 条记录")

----------------------
def find_files(self, versions: List[str], dates: List[str]) -> List[Path]:
    """
    生成有效文件路径列表，支持通配符（例如 "*"）
    :param versions: 版本列表（支持通配符*）
    :param dates: 日期列表（支持通配符*）
    :return: 有效文件路径列表
    """
    valid_files = []
    for version_pattern in versions:
        # 如果版本参数中包含通配符，则使用 glob 扩展匹配
        if "*" in version_pattern:
            version_dirs = [p for p in MAIN_FOLDER.glob(version_pattern) if p.is_dir()]
            if not version_dirs:
                logger.debug(f"没有匹配到版本目录: {version_pattern}")
                continue
        else:
            if not self._is_safe_path(version_pattern):
                logger.warning(f"跳过非法版本路径: {version_pattern}")
                continue
            version_dir = MAIN_FOLDER / version_pattern
            if not version_dir.exists():
                logger.debug(f"版本目录不存在: {version_dir}")
                continue
            version_dirs = [version_dir]
        
        for version_dir in version_dirs:
            for date_pattern in dates:
                # 同理，日期中如果包含通配符则使用 glob 进行扩展
                if "*" in date_pattern:
                    date_dirs = [p for p in version_dir.glob(date_pattern) if p.is_dir()]
                    if not date_dirs:
                        logger.debug(f"没有匹配到日期目录: {date_pattern} in {version_dir}")
                        continue
                else:
                    if not self._is_safe_path(date_pattern):
                        logger.warning(f"跳过非法日期路径: {date_pattern}")
                        continue
                    date_dir = version_dir / date_pattern
                    if not date_dir.exists():
                        logger.debug(f"日期目录不存在: {date_dir}")
                        continue
                    date_dirs = [date_dir]
                
                for date_dir in date_dirs:
                    target_dir = date_dir / REGION
                    if target_dir.exists() and target_dir.is_dir():
                        valid_files.extend(target_dir.glob("*.csv"))
                    else:
                        logger.debug(f"区域目录不存在: {target_dir}")
    
    return valid_files
