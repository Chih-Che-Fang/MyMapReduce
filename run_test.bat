dir /s /B src\*.java > sources.txt
javac -d bin -s bin -h bin @sources.txt

del "output_grep\out.*"
del "output_wordcount\out.*"
del "output_urlcount\out.*"
del "intermediate\tmp.*"


REM test for three application under 1 mapper fail
java -cp ".\bin" Apps.User user_config_wordcount 1
java -cp ".\bin" Apps.User user_config_urlcount 1
java -cp ".\bin" Apps.User user_config_grep 1
java -cp ".\bin" Utils.OutputComperator 2

del "output_grep\out.*"
del "output_wordcount\out.*"
del "output_urlcount\out.*"
del "intermediate\tmp.*"

REM test for three application under 1 reducer fail
java -cp ".\bin" Apps.User user_config_wordcount 2
java -cp ".\bin" Apps.User user_config_urlcount 2
java -cp ".\bin" Apps.User user_config_grep 2
java -cp ".\bin" Utils.OutputComperator 2

del "output_grep\out.*"
del "output_wordcount\out.*"
del "output_urlcount\out.*"
del "intermediate\tmp.*"

REM test for three application under no mapper/reducers fail
java -cp ".\bin" Apps.User user_config_wordcount 0
java -cp ".\bin" Apps.User user_config_urlcount 0
java -cp ".\bin" Apps.User user_config_grep 0
java -cp ".\bin" Utils.OutputComperator 2
pause