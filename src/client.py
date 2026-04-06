import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, UploadFile, File
import os
import json

load_dotenv()
app = FastAPI(
    docs_url=None,
    redoc_url=None,
    openapi_url=None
)

@app.post("/update-env")
async def update_env(
    file: UploadFile = File(...),
    x_token: str = Header(None)
):
    if x_token != os.getenv("ADMIN_KEY"):
        raise HTTPException(status_code=403, detail="Forbidden")

    content = await file.read()

    try:
        data = json.loads(content)
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not data:
        raise HTTPException(status_code=400, detail="Empty payload")


    env_data = {}

    if os.path.exists("../.env"):
        with open("../.env", "r") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    env_data[key] = value

    for key in ['WB_API_KEY', 'OZON_API_KEY', 'YANDEX_API_KEY']:
        if key not in data.keys():
            raise HTTPException(status_code=400, detail="Invalid payload")


    env_data.update(data)

    with open("../.env", "w") as f:
        for key, value in env_data.items():
            f.write(f"{key}={value}\n")

    return {"status": "success"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)