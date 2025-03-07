# TronLab Data Processing

这是一个用于处理TronLab数据的项目，使用了PySpark进行大规模数据处理和特征提取。项目包含多个Python脚本，每个脚本执行特定的数据处理任务。

## 项目结构

```
checkCreateContractAnd0.py
convertLabeledAddress.py
convertTronAddressToEvmAddress.py
deleteNoBlackInBlackFeatures.py
findSameAdressBetweenTwoCsv.py
get_easy_features.py
get_edge.py
get_features.py
get_other_features.py
get_white_future.py
getWhiteFromAllEasyfeature.py
graph_features.py
groupbyTransType.py
simpleWeb3ApiCall.py
```

## 脚本说明

- **checkCreateContractAnd0.py**: 计算createContract个数。
- **convertLabeledAddress.py**: 将Tron地址转换为EVM地址。
- **convertTronAddressToEvmAddress.py**: 提供Tron地址到EVM地址的转换函数。
- **deleteNoBlackInBlackFeatures.py**: 删除不在黑名单中的特征数据。
- **findSameAdressBetweenTwoCsv.py**: 查找两个CSV文件中相同的地址。
- **get_easy_features.py**: 提取简单特征。
- **get_edge.py**: 提取边数据。
- **get_features.py**: 提取各种特征数据。
- **get_other_features.py**: 提取涉及时间的特征数据。
- **get_white_future.py**: 作废脚本。
- **getWhiteFromAllEasyfeature.py**: 从所有简单特征中提取白名单特征。
- **graph_features.py**: 计算图特征。
- **groupbyTransType.py**: 按交易类型分组统计。
- **simpleWeb3ApiCall.py**: 简单的Web3 API调用测试。

## 运行环境

- Python 3.11
- PySpark
- Web3.py

## 使用方法

1. 克隆仓库到本地：
    ```sh
    git clone https://github.com/yourusername/TronLabDataProcessing.git
    ```

2. 安装依赖：
    ```sh
    pip install -r requirements.txt
    ```

3. 根据需要运行相应的脚本：
    ```sh
    python checkCreateContractAnd0.py
    ```

## 贡献

欢迎提交问题和贡献代码！请确保在提交PR之前运行所有测试。

## 许可证

本项目采用 MIT 许可证。详情请参阅 LICENSE 文件。

---

感谢您使用TronLab Data Processing项目！如果有任何问题或建议，请随时联系我。