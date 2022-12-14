from functools import lru_cache
import numpy as np
from pathlib import Path
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from joblib import load
import uvicorn
from schemas import FEATURE, PAYMENT_PROBA, feature_names
import os
import pickle
from pathlib import Path
import logging

# from .monitoring import instrumentator

LOGGER = logging.getLogger(__name__)
uvicorn_access = logging.getLogger("uvicorn.access")
uvicorn_access.disabled = True

LOGGER.setLevel(logging.DEBUG)

app = FastAPI(title="Prediction Model")


# model_path = os.path.join("artifacts/", "model.pkl")


@lru_cache(maxsize=1)
def get_model():
    model_path = Path(__file__).parent.parent / "artifacts" / "model.joblib"
    model = load(str(model_path))
    LOGGER.info(model_path)
    return model


@app.get("/")
async def docs():
    return RedirectResponse(url="/docs")


@app.post("/predict", response_model=PAYMENT_PROBA)
async def predict(data: FEATURE, response: Response):
    data = data.dict()
    features = np.array([data[f] for f in feature_names]).reshape(1, -1)
    model = get_model()
    prediction = round(model.predict_proba(features)[:, 1][0], 3)
    return PAYMENT_PROBA(proba=prediction)


@app.get("/healthcheck")
async def healthcheck():
    return {"status": "ok"}


if __name__ == "__main__":
    port = int(os.environ.get("CONF_SERVER_PORT", "5000"))
    uvicorn.run("app.api:app", host="0.0.0.0", port=port, reload=False)
