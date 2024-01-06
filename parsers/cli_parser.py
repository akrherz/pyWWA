"""Parse CLI text products

The CLI report has lots of good data that is hard to find in other products,
so we take what data we find in this product and overwrite the database
storage of what we got from the automated observations
"""
# Local
from pywwa.workflows.cli import main

if __name__ == "__main__":
    # Do Stuff
    main()
