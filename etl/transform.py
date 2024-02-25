import pandas as pd
import json
from datetime import datetime, timedelta


class DataTransformer:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def insert_columns(self):
        """Inserts new columns into the DataFrame for storing results."""
        self.df = self.df.assign(
            recent_claims_qty=None,
            disb_active_bank_wo_tbc=None,
            days_since_last_loan=None,
        )

    def transform_data(self, json_col):
        """
        Transforms the data based on the specified JSON column.
        """
        # Parse and expand JSON data
        json_data = self.df[json_col].apply(self.parse_JSON)
        expanded_df = pd.concat(
            [pd.json_normalize(data) for data in json_data], keys=self.df.index
        )

        # Apply transformations
        self.df["recent_claims_qty"] = expanded_df.groupby(level=0).apply(
            self.count_claims_within_period
        )
        self.df["recent_claims_qty"] = self.df["recent_claims_qty"].fillna("-3")
        self.df["disb_active_bank_wo_tbc"] = expanded_df.groupby(level=0).apply(
            self.sum_disb_loans
        )
        self.df["disb_active_bank_wo_tbc"] = self.df["disb_active_bank_wo_tbc"].fillna(
            "-3"
        )
        self.df["days_since_last_loan"] = expanded_df.groupby(level=0).apply(
            self.days_since_last_loan
        )
        self.df["days_since_last_loan"] = self.df["days_since_last_loan"].fillna("-3")

        return self.df

    @staticmethod
    def parse_JSON(json_data):
        """Parses JSON data."""
        try:
            if json_data == None or not (isinstance(json_data, str)) or json_data == "":
                return []
            else:
                return json.loads(json_data) if json_data else []
        except json.JSONDecodeError:
            return []

    @staticmethod
    def count_claims_within_period(group_df):
        """Counts claims within the past 180 days."""
        starting_date = datetime.now() - timedelta(days=180)
        return group_df[
            group_df["claim_date"] >= starting_date.strftime(r"%d.%m.%Y")
        ].shape[0]

    @staticmethod
    def sum_disb_loans(group_df):
        """Sums the loans excluding specific banks."""
        bad_vals = ["LIZ", "LOM", "MKO", "SUK", None]

        filtered_loans = DataTransformer.filter_and_validate_loans(
            group_df,
            loan_col="loan_summa",
            bank_col="bank",
            bad_values=bad_vals,
        )
        if filtered_loans is None:
            return "-1"
        return filtered_loans["loan_summa"].sum()

    @staticmethod
    def days_since_last_loan(group_df):
        """Calculates days since the last loan."""
        group_df["contract_date"] = pd.to_datetime(
            group_df["contract_date"], format=r"%d.%m.%Y", errors="coerce"
        )
        last_loan_date = group_df["contract_date"].max()
        if type(last_loan_date) == pd._libs.tslibs.nattype.NaTType:
            return "-1"
        return int((datetime.now() - last_loan_date).days)

    @staticmethod
    def drop_nan(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Removes rows with NaN or empty values in the specified column.
        """
        dropped_null = df[df[column].notna()]
        dropped_empty_str = dropped_null[dropped_null[column] != ""]
        return dropped_empty_str

    @staticmethod
    def filter_and_validate_loans(loans, loan_col, bank_col, bad_values):
        """
        Filters and validates loan data, excluding specified banks and invalid entries.
        """
        loans = DataTransformer.drop_nan(loans, loan_col)
        loans = loans[loans[loan_col] != 0]
        if bank_col not in loans or len(loans) < 1:
            return None
        loans = DataTransformer.drop_nan(loans, bank_col)
        loans = loans[~loans[bank_col].isin(bad_values)]
        return loans if len(loans) > 0 else None
