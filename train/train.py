import logging
from pathlib import Path
import pickle
import pandas as pd
from joblib import dump
from sklearn import preprocessing
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import lightgbm as lgb

logger = logging.getLogger(__name__)


def prepare_dataset(test_size=0.2, random_seed=1):
    cur_dir = Path(__file__).parent.parent.absolute()
    datasets_dir = "datasets/data.csv"
    datasets_path = cur_dir / datasets_dir
    dataset = pd.read_csv(datasets_path)
    target_variable = "paid_num"
    X = dataset.drop(columns=target_variable)
    y = dataset[target_variable]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_seed
    )
    return {"X_train": X_train, "X_test": X_test, "y_train": y_train, "y_test": y_test}


def train():
    logger.info("Preparing dataset...")
    dataset = prepare_dataset()
    # This is an example
    X_train = dataset["X_train"]
    y_train = dataset["y_train"]

    X_test = dataset["X_test"]
    y_test = dataset["y_test"]

    clf = lgb.LGBMClassifier()
    clf.fit(X_train, y_train)

    # separate features from target
    y_pred = clf.predict(X_test)
    logger.info(classification_report(y_test, y_pred))

    logger.info("Saving artifacts...")
    Path("artifacts").mkdir(exist_ok=True)
    dump(clf, "artifacts/model.joblib")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    train()
