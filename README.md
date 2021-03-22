# mapreduce-Chih-Che-Fang  


#Enviornment: Windows + Java SDK 8 Installed  
#Applications: WordCount, Grep, URLCount (Please refer MapReduce paper for more detail)

# How to run?  
1. Switch to the root directory of this project (Ex. cd /mapreduce-Cih-Che-Fang) and confirm the path contains no "blank"  
2. Perform run_test.bat on Windows OS (With JDK installed and with JDK environment variable set), and will output all the file to "output_[APP_NAME]" folder. (Ex. wordcount example will output result to output_wordcount folder, grep example will output to output_grep folder)
3. See the testing result on console, it will tell you if the output is equal to the Spark output. Logs like:  

C:\Users\user\mapreduce-Chih-Che-Fang>java -cp ".\bin" Utils.OutputComperator 2
Application:wordcount
Testing Result: Scuccess, output files matches expected file!
Application:urlcount
Testing Result: Scuccess, output files matches expected file!
Application:grep
Testing Result: Scuccess, output files matches expected file!


# Directory/Files Description
-	Bin: Complied JAVA class
-	SparkValidation: Source code of Spark validation program for comparing output
-	Src: Project source code
-	Run_test.bat: testing script
-	Local_refresh: For local debugging use
-	Docs: Design documents
-	Input_grep: Input files of application “Distributed Grep”
-	Output_grep: Output files of application “Distributed Grep”
-	Input_wordcount: Input files of application “Word Count”
-	Output_wordcount: Output files of application “Word Count”
-	Input_urlcount: Input files of application “Count of URL Access Frequency”
-	Output_urlcount: Output files of application “Count of URL Access Frequency”
-	Intermediate: Intermediate files
-	Read.md: Readme file
