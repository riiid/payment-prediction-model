from typing import List
from pydantic import BaseModel
from typing_extensions import TypedDict

feature_names = [
    "mean_radius",
    "mean_texture",
    "mean_perimeter",
    "mean_area",
    "mean_smoothness",
]


# class Interaction(TypedDict):
#     mean_radius: float
#     mean_texture: float
#     mean_perimeter: float
#     mean_area: float
#     mean_smoothness: float


# class Payment_probability(TypedDict):
#     # auth_id:str, 이건 나중에
#     proba: float


class FEATURE(BaseModel):
    mean_radius: float
    mean_texture: float
    mean_perimeter: float
    mean_area: float
    mean_smoothness: float


class PAYMENT_PROBA(BaseModel):
    proba: float
