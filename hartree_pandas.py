from typing import List
import pandas as pd

def load_and_merge_datasets()->pd.DataFrame:
    df_dataset1 = pd.read_csv("data\\dataset1.csv")
    df_dataset2 = pd.read_csv("data\\dataset2.csv")
    
    # Merge the datasets on the 'counter_party' column using a left join
    merged_df = pd.merge(left=df_dataset1, right=df_dataset2, on="counter_party", how="left")
    
    return merged_df

def process_data(df:pd.DataFrame) -> List[pd.DataFrame]:
    # Select the required columns
    df = df[["legal_entity", "counter_party", "tier", "rating", "value", "status"]]
    
    # Define group-by columns
    group_by_columns = ["legal_entity", "counter_party", "tier"]
    
    # Calculate the maximum rating by counter_party
    max_rating = df.groupby(group_by_columns)["rating"].max().reset_index()
    max_rating.rename(columns={"rating": "max(rating by counterparty)"}, inplace=True)
    
    # Calculate the sum of values where status is "ARAP"
    sum_arap = df[df["status"] == "ARAP"].groupby(group_by_columns)["value"].sum().reset_index()
    sum_arap.rename(columns={"value": "sum(value where status=ARAP)"}, inplace=True)
    
    # Calculate the sum of values where status is "ACCR"
    sum_accr = df[df["status"] == "ACCR"].groupby(group_by_columns)["value"].sum().reset_index()
    sum_accr.rename(columns={"value": "sum(value where status=ACCR)"}, inplace=True)
    
    # Calculate the total count of values
    count_values = df.groupby(group_by_columns)["value"].count().reset_index()
    count_values.rename(columns={"value": "Total(count with same legal_entity, counter_party,tier)"}, inplace=True)
    
    return max_rating, sum_arap, sum_accr, count_values

def merge_dataframes(dataframes: List[pd.DataFrame], on: List) -> pd.DataFrame:
    merged = None
    for dataframe in dataframes:
        if merged is not None:
            merged = merged.merge(right=dataframe, on=on, how="outer")
        else:
            merged = dataframe
    return merged

def main():
    merged_df = load_and_merge_datasets()
    max_rating, sum_arap, sum_accr, count_values = process_data(merged_df)
    dataframes = [max_rating, sum_arap, sum_accr, count_values]
    result = merge_dataframes(dataframes, on=["legal_entity", "counter_party", "tier"])
    
    # Fill missing values with 0 and drop duplicates
    result.fillna(0, inplace=True)
    result.drop_duplicates(inplace=True)
    
    print(result)

if __name__ == "__main__":
    main()
