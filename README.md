1 - Go to this script to prepare data combinationMultiTargetMultiPosibleLeftWithProgress.R

2 - Edit the Right field, the left field and the targets field as you like
- No Spaces in the right field
- The fields are in the upper part of the script

3 - Run it (If there is a lot of targets it can takes a lot of time)

4 - It will generate csv file in c:\qpadmdata\
- you can change the path if you want but do it on both ends
- Create the folfer before running the script
- 4 Files will be generated. One for the right, one for the weights, One for the avg se and one for the p-value

5 - When the data are ready, go to this file generatePlotFromCsvLargeData.R

6- Run it, it will use the csv files generated in c:\qpadmdata\ to generate plots in c:\qpadmgraphics
- create the folder before generating the plot

The other files are just different attempts to do the same thing

Be aware of the path of the input datasets in the beginning of the scripts and the paths of the output also in the scripts
