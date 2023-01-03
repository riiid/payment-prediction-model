from typing import List
from pydantic import BaseModel
from typing_extensions import TypedDict

feature_names = [
    "agree_marketing",
    "agree_push",
    "access_cnt",
    "diag_score",
    "target_score",
    "score_diff_td",
    "first_cell_type",
    "first_cell_score",
    "last_cell_score",
    "score_diff_ld",
    "cell_cnt",
    "cycle_cnt",
    "basics",
    "lessons",
    "mock_exams",
    "my_note_quizzes",
    "questions",
    "reviews",
    "vocab",
    "self_lessons",
    "self_questions",
    "self_virt_exams",
    "self_vocab",
    "season",
    "arppu",
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
    agree_marketing: bool
    agree_push: bool
    access_cnt: float
    diag_score: float
    target_score: float
    score_diff_td: float
    first_cell_type: int
    first_cell_score: float
    last_cell_score: float
    score_diff_ld: float
    cell_cnt: int
    cycle_cnt: int
    basics: int
    lessons: int
    mock_exams: int
    my_note_quizzes: int
    questions: int
    reviews: int
    vocab: int
    self_lessons: int
    self_questions: int
    self_virt_exams: int
    self_vocab: int
    season: int
    arppu: float


class PAYMENT_PROBA(BaseModel):
    proba: float
