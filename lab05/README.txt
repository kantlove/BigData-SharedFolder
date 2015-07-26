+==================================================================
	 _       ___  ______  _____ _____ 
	| |     / _ \ | ___ \|  _  |  ___|
	| |    / /_\ \| |_/ /| |/' |___ \ 
	| |    |  _  || ___ \|  /| |   \ \
	| |____| | | || |_/ /\ |_/ /\__/ /
	\_____/\_| |_/\____/  \___/\____/  
	
	Course: BigData
	Author: Minh Thai
+==================================================================

-------------------------------------------------------------------
 How to run
-------------------------------------------------------------------
  Run this command
    bin/hadoop jar AssociationRule_Step3.jar input k1 k2 rules
    where:
      - input: input folder path
      - k1: output folder after step 1
      - k2: output folder after step 2
      - rules: output folder after step 3

-------------------------------------------------------------------
 JAR files
-------------------------------------------------------------------
  There are 3 jar files in the package:
    - AssociationRule_Step1.jar
	This is the 1st pass of Support.
    - AssociationRule_Step2.jar
	This file contains both 1st and 2nd pass of Support.
    - AssociationRule_Step3.jar
	This is the complete solution, contains all 3 steps of 
	the solution (1st and 2nd pass of Support, Confidence).

-------------------------------------------------------------------
 Source files
-------------------------------------------------------------------
  See 'source' folder.

-------------------------------------------------------------------
 Output
-------------------------------------------------------------------
  See 'output' folder.
  This folder contains 4 files:
    - 3 files for 3 steps of the solution.
    - A text file contains the most interested rules.