import pandas as pd

def xlsx_to_csv(input_file, output_file):
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Write the DataFrame to a CSV file
    df.to_csv(output_file, index=False)


if __name__ == "__main__":

    inputfile = "/home/eirik/data/terrorist_dataset/globalterrorismdb_0522dist.xlsx"
    outputfile = "/home/eirik/data/terrorist_dataset/globalterrorismdb_0522dist.csv"

    xlsx_to_csv(inputfile, outputfile)