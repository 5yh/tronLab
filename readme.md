# TronLab Data Processing

[中文版本](./readmeCN.md)

This is a project for processing TronLab data using PySpark for large-scale data processing and feature extraction. The project includes multiple Python scripts, each performing specific data processing tasks.

## Project Structure

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

## Script Descriptions

- **checkCreateContractAnd0.py**: Calculate the number of createContract.
- **convertLabeledAddress.py**: Convert Tron addresses to EVM addresses.
- **convertTronAddressToEvmAddress.py**: Provide a function to convert Tron addresses to EVM addresses.
- **deleteNoBlackInBlackFeatures.py**: Delete features not in the blacklist.
- **findSameAdressBetweenTwoCsv.py**: Find the same addresses between two CSV files.
- **get_easy_features.py**: Extract simple features.
- **get_edge.py**: Extract edge data.
- **get_features.py**: Extract various feature data.
- **get_other_features.py**: Extract time-related feature data.
- **get_white_future.py**: Deprecated script.
- **getWhiteFromAllEasyfeature.py**: Extract whitelist features from all simple features.
- **graph_features.py**: Calculate graph features.
- **groupbyTransType.py**: Group and count transactions by type.
- **simpleWeb3ApiCall.py**: Simple Web3 API call test.

## Environment

- Python 3.11
- PySpark
- Web3.py

## Usage

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/TronLabDataProcessing.git
    ```

2. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```

3. Run the desired script:
    ```sh
    python checkCreateContractAnd0.py
    ```

## Contributing

Feel free to submit issues and contribute code! Please ensure all tests pass before submitting a PR.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

---

Thank you for using the TronLab Data Processing project! If you have any questions or suggestions, please feel free to contact me.