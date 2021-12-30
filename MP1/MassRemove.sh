grp="39"
netid="yuhangr2"
gitUsername="yuhangr2"
gitPassword="3381008RYhQQ"
gitRepoLink="gitlab.engr.illinois.edu/yuhangr2/mp1_ece428_yuhangr2"
for i in {1..8}
do
    echo "*******************************************************************************************"
    echo "***********************************Removing is Server $i **********************************"
    ssh $netid@sp21-cs425-g`printf $grp`-`printf %02d $i`.cs.illinois.edu "rm -rf mp1_ece428_yuhangr2"
done