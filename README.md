# FAIR_champion
A python package to identify a champion of FAIR data management practices from a list of publications.

### Author
Graham Thomas

### Description
This project was inspired by the ELIXIR All Hands meeting 2025 hosted in Exeter, UK. The goal is to create a tool that can analyse a list of scientific publications and identify the one that best exemplifies FAIR (Findable, Accessible, Interoperable, Reusable) data management practices. The tool can be applied to any list of publications, but the primary idea is to apply it to the Monthly Biosciences Newsletter, which features recent publications in the Biosciences department at the University of Exeter.

### Aims
- This is an open project intended as a learning exercise for those interested in coding and data science. 
- It will be shared on the bioinformatexe github page for members of that network to contribute to and learn from.
- It is a playground for community members to practice their github flow, posting and closing issues.
- An opportunity to work collaboratively on a real project that will produce a useful output.
- This project is an exercise for me to learn python, package development, text mining, API calls etc.

### Current state
Incomplete

Before starting this project my naieve idea was to extract the publications from the monthly Biosciences Newsletter, then use the DOI of each publication to retrieve metadata from an API such as EuropePMC or CrossRef. I was hoping that the metadata would include information about data availability statements or links to datasets, which could be used to assess the FAIRness of each publication. However, after exploring the metadata available from these APIs, I found the data availability statements aren't included in the metadata. This means that a more sophisticated approach is needed to evaluate the FAIRness of each publication. My current thought is to either download the full article where possible and extract the relevant section from there, or do a web-scraping for the data availablity section. I am currently stuck working on consistently retrieving the data availability statements from each publication. Once I have accomplished this, I can then go back to implementing a scoring system to identify the FAIR champion. My current issue is dealing with different publishers. My approach has been to use a simple single entry input file to test the response from each publisher. This approach has the limitation of having to optimise for each publisher. I have had success with elsevier where full text xml is available. These input files are saved in `data/test-1.txt` up to `data/test-5.txt` and then `test-6.txt` contains all 5 papers.