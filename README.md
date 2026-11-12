# 🧹 CleanPipe: Resilient Schema Validator & ETL Pipeline

**Category**: Data Engineering & Automation  
**Author**: Hashim ([@hashim_py](https://x.com/hashim_py) · [hashim.me.uk](https://hashim.me.uk))  
**Status**: Live / Production Ready  

---

## 🎯 What it does
CleanPipe is a lightweight data cleansing and schema validation pipeline built for Python environments where incoming customer or transactional records need strict normalization before hitting relational databases (PostgreSQL/MySQL) or data warehouses.

### Key Capabilities:
1. **Defensive Schema Rules**: Strict type coercion for integers, floats, emails, and multi-format timestamps.
2. **Anomaly Quarantine**: Separates corrupted or out-of-boundary records into an error audit log instead of failing entire batches.
3. **Currency & String Sanitization**: Strips localized formatting (`$`, commas, extra whitespace) automatically.
4. **Zero-Bloat Execution**: Implemented strictly with Python standard library modules for sub-millisecond per-record latency.

---

## 🛠️ Architecture & Tech Stack
- **Language**: Python 3.10+
- **Modules**: `dataclasses`, `datetime`, `re`
- **Telemetry**: Zero external network dependencies, 100% private & local execution

---

## 🚀 Getting Started

### 1. Run the Interactive CLI Demo
```bash
python main.py
```

### 2. Run Test Suite
```bash
python test_pipe.py
```

---

## 🔗 Live Links & Portfolio
- Developer Portfolio & Live Web Tools: [https://hashim.me.uk](https://hashim.me.uk)
- Author on X: [@hashim_py](https://x.com/hashim_py)
