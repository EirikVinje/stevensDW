import polars as pl
import pandas as pd
import json
import csv


def smaller(input_file, output_file):
    
    df = pl.read_csv(input_file, infer_schema_length=0)
    
    with open("columns.json", "rb") as f:
            columns = json.load(f)["columns"]

    df = df[columns]

    df.write_csv(output_file)    


if __name__ == "__main__":

    inputfile = "/home/eirik/data/terrorist_dataset/globalterrorismdb_0522dist.csv"
    outputfile = "/home/eirik/projects/stevensDW/data/globalterrorismdb_0522dist.csv"

    smaller(inputfile, outputfile)