#========================================================================
# BigData - Lab 04
# Author: MinhThai
#========================================================================

#------------------------------------------------------------------------
# Excercise 1
#------------------------------------------------------------------------
# Input file
	matrix.txt, vector.txt
# Vector format
	[val_0] [val_1] ...
# Matrix format
	[row_id] [val_0] [val_1] ...
# How to run
	VectorMatrixMul.jar [matrix_folder] [output_folder] [vector_path]

	Example:
	VectorMatrixMul.jar input output vector/vector.txt

#------------------------------------------------------------------------
# Excercise 2
#------------------------------------------------------------------------
# Input file
	2matrices.txt
# Matrix format
	[matrix_id] [row_id] [val_0] [val_1] ...
# How to run
	GeneralMatrixMul.jar [matrix_folder] [middle_folder] [output_folder]
	(middle_folder: output of the 1st pass a.k.a step 1)

	Example:
	GeneralMatrixMul.jar input middle output

