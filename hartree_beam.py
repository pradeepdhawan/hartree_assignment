import os
import apache_beam as beam


class CustomSumAndMaxFn(beam.CombineFn):
    def create_accumulator(self):
        return {
            "legal_entity": None,
            "counter_party": None,
            "tier": int(0),
            "max(rating by counterparty)": int(0),
            "sum(value where status=ARAP)": float(0),
            "sum(value where status=ACCR)": float(0),
            "Total(count with same legal_entity, counter_party,tier)": int(0)
        }

    def add_input(self, accumulator_dict, input_value):
        legal_entity, counter_party, tier, rating, status, value = input_value

        accumulator_dict["legal_entity"] = legal_entity
        accumulator_dict["counter_party"] = counter_party
        accumulator_dict["tier"] = tier
        accumulator_dict["Total(count with same legal_entity, counter_party,tier)"] +=1

        if "sum(value where status=" + status + ")" in accumulator_dict.keys():
            accumulator_dict["sum(value where status=" + status + ")"] += value
        else:
            accumulator_dict["sum(value where status=" + status + ")"] = value

        accumulator_dict["max(rating by counterparty)"] = max(accumulator_dict["max(rating by counterparty)"], rating)
        return accumulator_dict

    def merge_accumulators(self, accumulator_dicts):
        merged_accumulator_dict = self.create_accumulator()
        for accumulator_dict in accumulator_dicts:
            merged_accumulator_dict["legal_entity"] = accumulator_dict["legal_entity"]
            merged_accumulator_dict["counter_party"] = accumulator_dict["counter_party"]
            merged_accumulator_dict["tier"] = accumulator_dict["tier"]
            merged_accumulator_dict["max(rating by counterparty)"] = max(
                merged_accumulator_dict["max(rating by counterparty)"],
                accumulator_dict["max(rating by counterparty)"])
            merged_accumulator_dict["sum(value where status=ARAP)"] += accumulator_dict["sum(value where status=ARAP)"]
            merged_accumulator_dict["sum(value where status=ACCR)"] += accumulator_dict["sum(value where status=ACCR)"]
            merged_accumulator_dict["Total(count with same legal_entity, counter_party,tier)"] += accumulator_dict["Total(count with same legal_entity, counter_party,tier)"]
        return merged_accumulator_dict

    def extract_output(self, accumulator_dict):
        return f'{accumulator_dict["legal_entity"]}, \
            {accumulator_dict["counter_party"]}, \
            {accumulator_dict["tier"]}, \
            {accumulator_dict["max(rating by counterparty)"]}, \
            {accumulator_dict["sum(value where status=ARAP)"]}, \
            {accumulator_dict["sum(value where status=ACCR)"]}, \
            {accumulator_dict["Total(count with same legal_entity, counter_party,tier)"]}'
        #return accumulator_dict


with beam.Pipeline() as pipeline:
    dataset1 = (
        pipeline
        | "Read Dataset1"
        >> beam.io.ReadFromText("data/dataset1.csv", skip_header_lines=1)
        | "Parse Dataset1" >> beam.Map(lambda line: line.split(","))
    )

    dataset2 = (
        pipeline
        | "Read Dataset2"
        >> beam.io.ReadFromText("data/dataset2.csv", skip_header_lines=1)
        | "Parse Dataset2" >> beam.Map(lambda line: line.split(","))
    )

    dataset1_keyed = dataset1 | "Key by Counterparty dataset 1" >> beam.Map(
        lambda row: (row[2], row)
    )

    dataset2_keyed = dataset2 | "Key by Counterparty  dataset 2" >> beam.Map(
        lambda row: (row[0], row)
    )

    # Group and join dataset1 and dataset2 on 'counter_party'
    grouped_data = {
        "dataset1": dataset1_keyed,
        "dataset2": dataset2_keyed,
    } | "CoGroup Join" >> beam.CoGroupByKey()

    def combine_data_for_datasets(element):
        _, data = element
        dataset1_data = data["dataset1"]
        dataset2_data = data["dataset2"]

        for set1 in dataset1_data:
            _, legal_entity, counter_party, rating, status, value = set1
            for set2 in dataset2_data:
                counter_party, tier = set2
                yield (legal_entity, counter_party, int(tier), int(rating), status, float(value))

    joined_data = grouped_data | "Combine Data" >> beam.FlatMap(
        combine_data_for_datasets
    )

    # Group and calculate the sum of 'rating' and max of 'value' by 'legal_entity', 'counter_party', and 'tier'
    # with condition of status
    grouped_data = (
        joined_data
        | "Group by Key" >> beam.Map(lambda x: ((x[0], x[1], x[2]), x))
        | "Group and Combine" >> beam.CombinePerKey(CustomSumAndMaxFn())
        | "Only Value" >> beam.Map(lambda x: x[1])
    )
    #    | 'Count of elements' >> beam.combiners.Count.Globally())
    #    | 'Remove duplicates' >> beam.Distinct())
    output_file = "output/result.csv"
    if os.path.exists(output_file + "-00000-of-00001.csv"):
        os.remove(output_file + "-00000-of-00001.csv")

    grouped_data | beam.LogElements()
    grouped_data | "Write to outputfile" >> beam.io.WriteToText(output_file, file_name_suffix='.csv', header="legal_entity,counter_party, tier,max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR), Total(count with same legal_entity, counter_party,tier)")

    
