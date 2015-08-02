+==================================================================
	   __    _      ___  ___   __   
	  / /   /_\    / __\/ _ \ / /_  
	 / /   //_\\  /__\// | | | '_ \ 
	/ /___/  _  \/ \/  \ |_| | (_) |
	\____/\_/ \_/\_____/\___/ \___/ 
	
	Course: BigData
	Author: Minh Thai
+==================================================================

-------------------------------------------------------------------
 How to run
-------------------------------------------------------------------
  Easy way:
    Run the run.sh script 
    (for more information on what it does, please open that file)
  Hard way: run this command
    bin/hadoop jar AssociationRule_Step3.jar [dimension] [dataset]
	[center] [input] [output] [number of rounds]
    where:
      - dimension: dimensions of the input vector
      - dataset: path to dataset.txt folder
      - center: path to centers.txt folder
      - input: path to kmeans_input.txt folder

-------------------------------------------------------------------
 How to generate new input
-------------------------------------------------------------------
  Install python.
  Run the generator/data_generator.py file
    - Specify some config in that files if you want
    - You MUST NOT change the file names

-------------------------------------------------------------------
 Program flow
-------------------------------------------------------------------
  Read centers.txt -> Round 1 -> Write to centers.txt
	-> Copy content from output to input -> Round 2 -> ... 

-------------------------------------------------------------------
 Input file explain
-------------------------------------------------------------------
  dataset.txt
    This file contains all points in our database
  centers.txt
    This file contains initial chosen centroid (chose randomly)
  kmeans_input.txt
    This file contains the initial centers and some random
    children

-------------------------------------------------------------------
 Source files
-------------------------------------------------------------------
  See 'source' folder.

-------------------------------------------------------------------
 Jar file
-------------------------------------------------------------------
  See 'jar' folder.

-------------------------------------------------------------------
 Output files
-------------------------------------------------------------------
  See 'output' folder.
  There are 2 files:
    - centers.txt: all centroids at the end of the program
    - output: output of the program
      + Format: centroid	child_1 child_2 ....