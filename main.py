import src.extract as extract
import src.app as app

def main():
    extract.pull() # command runs the extract file which extracts out csv from API
    
    app.app.run(debug=True) # command runs the app where dataframes are created and rest APIs are made to access to get results of queries given, over a webpage


if __name__ == "__main__":
    main()
