Share


You said:
import os
from pathlib import Path
import pandas as pd
import pyarrow.csv as pv
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Optional, Set
import logging
import time
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MAIN_FOLDER = Path("main_folder")
REGION = "region"
SAFE_PATH_PATTERN = re.compile(r'^[\w\-\.]+$')  # 防止路径遍历攻击

def validate_input(*paths: str) -> bool:
    """防止路径遍历攻击和非法字符"""
    return all(SAFE_PATH_PATTERN.match(p) for p in paths)

def get_file_paths(versions: List[str], dates: List[str]) -> List[str]:
    """使用生成器优化大规模路径遍历"""
    if not all(validate_input(v, d) for v in versions for d in dates):
        raise ValueError("Invalid version or date format")
    
    for version in versions:
        version_path = MAIN_FOLDER / version
        if not version_path.exists():
            continue
            
        for date in dates:
            date_path = version_path / date / REGION
            if date_path.exists():
                yield from (str(p) for p in date_path.glob("*.csv"))

def fast_check_columns(file_path: str, required_cols: Set[str]) -> bool:
    """快速检查CSV文件列名而不加载全量数据"""
    try:
        schema = pv.read_csv(
            file_path,
            read_options=pv.ReadOptions(skip_rows=0, autogenerate_column_names=False)
        ).schema
        return required_cols.issubset(schema.names)
    except Exception as e:
        logger.debug(f"Column check failed for {file_path}: {e}")
        return False

def read_csv_file(file_path: str, columns_list: List[str], column_types: Dict[str, str]) -> Optional[pd.DataFrame]:
    """使用PyArrow原生类型转换优化内存使用"""
    try:
        # 显式关闭PyArrow多线程以避免资源竞争
        table = pv.read_csv(
            file_path,
            read_options=pv.ReadOptions(use_threads=False),
            convert_options=pv.ConvertOptions(
                column_types=column_types,
                include_columns=columns_list
            )
        )
        
        # 直接转换到Pandas避免中间对象
        return table.to_pandas(split_blocks=True, self_destruct=True)
    
    except Exception as e:
        logger.error(f"Critical error reading {file_path}: {e}", exc_info=True)
        return None

def parallel_read(
    file_paths: List[str],
    columns_list: List[str],
    column_types: Dict[str, str],
    max_workers: int = None
) -> pd.DataFrame:
    """基于进程池的并行读取，优化CPU密集型任务"""
    required_cols = set(columns_list)
    valid_paths = [p for p in file_paths if fast_check_columns(p, required_cols)]
    
    if not valid_paths:
        return pd.DataFrame()
    
    # 动态调整进程池大小
    max_workers = max_workers or min(os.cpu_count() or 1, len(valid_paths))
    logger.info(f"Processing {len(valid_paths)} files with {max_workers} workers")
    
    # 使用进程池规避GIL限制
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(read_csv_file, p, columns_list, column_types): p for p in valid_paths}
        
        results = []
        for future in as_completed(futures):
            file_path = futures[future]
            try:
                df = future.result()
                if df is not None:
                    results.append(df)
                    # 及时释放内存
                    if len(results) % 100 == 0:
                        pd.concat(results, copy=False).reset_index(drop=True)
                        results.clear()
            except Exception as e:
                logger.error(f"Failed processing {file_path}: {e}")
    
    return pd.concat(results, ignore_index=True, copy=False) if results else pd.DataFrame()

def main(
    versions: List[str],
    dates: List[str],
    columns_list: List[str],
    column_types: Dict[str, str],
    max_workers: int = None
) -> pd.DataFrame:
    """全流程优化版本"""
    start_time = time.perf_counter()
    memory_start = psutil.Process().memory_info().rss // 1024**2  # 需要import psutil
    
    try:
        file_paths = list(get_file_paths(versions, dates))
        if not file_paths:
            logger.warning("No valid files found")
            return pd.DataFrame()
        
        logger.info(f"Found {len(file_paths)} potential files")
        
        df = parallel_read(file_paths, columns_list, column_types, max_workers)
        
        time_elapsed = time.perf_counter() - start_time
        memory_used = psutil.Process().memory_info().rss // 1024**2 - memory_start
        logger.info(f"Processed {len(df)} rows in {time_elapsed:.2f}s | Memory: +{memory_used}MB")
        return df
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}", exc_info=True)
        raise

# 示例用法
if __name__ == "__main__":
    import psutil  # 需安装psutil
    
    column_types = {
        'column_a': 'string',
        'column_b': 'float64',
        'column_c': 'date32[ms]',
    }
    
    try:
        result = main(
            versions=["1", "2"],
            dates=["2023-10-01", "2023-10-02"],
            columns_list=["column_a", "column_b"],
            column_types=column_types,
            max_workers=8
        )
        print(result.info(memory_usage='deep'))
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
def parallel_read(...):
    required_cols = set(columns_list)
    valid_paths = [p for p in file_paths if fast_check_columns(p, required_cols)]
    if not valid_paths:
        return pd.DataFrame()

    max_workers = max_workers or min(os.cpu_count() or 1, len(valid_paths))
    logger.info(f"Processing {len(valid_paths)} files with {max_workers} workers")
    
    chunks = []      # To store intermediate concatenated chunks
    results = []     # To store individual DataFrames temporarily

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(read_csv_file, p, columns_list, column_types): p for p in valid_paths}
        for future in as_completed(futures):
            file_path = futures[future]
            try:
                df = future.result()
                if df is not None:
                    results.append(df)
                    # Every 100 files, concatenate and store the chunk, then clear results.
                    if len(results) >= 100:
                        chunk_df = pd.concat(results, copy=False).reset_index(drop=True)
                        chunks.append(chunk_df)
                        results.clear()
            except Exception as e:
                logger.error(f"Failed processing {file_path}: {e}")

    # Concatenate any remaining DataFrames
    if results:
        chunks.append(pd.concat(results, copy=False).reset_index(drop=True))
    
    # Final concatenation of all chunks
    return pd.concat(chunks, ignore_index=True, copy=False) if chunks else pd.DataFrame()
import pyarrow.csv as pa_csv
from pyarrow.csv import ReadOptions, ConvertOptions
from concurrent.futures import ThreadPoolExecutor
import logging

def check_columns(path: str, required_cols: list) -> bool:
    """无数据读取的快速列检查"""
    try:
        # 仅读取第一行元数据
        table = pa_csv.read_csv(
            path,
            read_options=ReadOptions(skip_rows=1, autogenerate_column_names=False)
        )
        return set(required_cols).issubset(table.schema.names)
    except Exception as e:
        logging.debug(f"Column check failed for {path}: {e}")
        return False

def process_data_batch(file_paths: list, required_cols: list) -> pd.DataFrame:
    # 第一阶段：并行过滤有效文件
    with ThreadPoolExecutor(max_workers=4) as executor:
        valid_mask = list(executor.map(
            lambda p: check_columns(p, required_cols),
            file_paths
        ))
    valid_paths = [p for p, valid in zip(file_paths, valid_mask) if valid]
    
    # 第二阶段：处理有效文件
    tables = []
    for path in valid_paths:
        try:
            table = pa_csv.read_csv(
                path,
                convert_options=ConvertOptions(
                    include_columns=required_cols,
                    column_types=column_types
                )
            )
            tables.append(table)
        except Exception as e:
            logging.error(f"Failed to process {path}: {e}")
    
    return pa.concat_tables(tables).to_pandas() if tables else pd.DataFrame()
