# Runbook

## 1. Create a virtual environment
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

## 2. Install dependencies
```powershell
pip install -r requirements.txt
```

## 3. Configure environment
Copy `.env.example` to `.env` and fill in the values.

```powershell
Copy-Item .env.example .env
```

## 4. Run the bot
```powershell
python bot.py
```

## 5. Run tests
```powershell
pytest -q
```
