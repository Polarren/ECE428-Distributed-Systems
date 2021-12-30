
grp="39"
netid="yuhangr2"
folderName="ece428_mp1_yuhangr2"
gitUsername="yuhangr2"
gitPassword="3381008RYhHH"
gitRepoLink="gitlab.engr.illinois.edu/yuhangr2/ece428_mp1_yuhangr2"
for i in {1..10}
do
    echo "*******************************************************************************************"
    echo "***********************************Git Pull at Server $i ***********************************"
    ssh $netid@sp21-cs425-g`printf $grp`-`printf %02d $i`.cs.illinois.edu "cd $folderName; git checkout main; git pull; exit"
done
