grp="39"
netid="yuhangr2"
folderName="ece428_mp1_yuhangr2"
gitUsername="yuhangr2"
gitPassword="3381008RYhHH"
gitRepoLink="gitlab.engr.illinois.edu/yuhangr2/ece428_mp1_yuhangr2"
# i =1
# ssh $netid@sp21-cs425-g`printf $grp`-`printf %02d $i`.cs.illinois.edu "cd $folderName;python3 logger.py 1234"
for i in {2..4}
do
    echo "*******************************************************************************************"
    echo "***********************************Running is Server $i **********************************"
    ssh $netid@sp21-cs425-g`printf $grp`-`printf %02d $i`.cs.illinois.edu "cd $folderName; exit"
done