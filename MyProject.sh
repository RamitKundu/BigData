#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[37m"` #Blue
    NUMBER=`echo "\033[37m"` #yellow
    FGRED=`echo "\033[37m"`
    RED_TEXT=`echo "\033[37m"`
    ENTER_LINE=`echo "\033[37m"`
    echo -e "${MENU}************************************H1B APPLICATIONS*********************************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1a) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1b) ${MENU} Find top 5 job titles who are having highest average growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2a) ${MENU} Which part of the US has the most no. Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2b) ${MENU} find top 5 locations in the US who have got certified visa for each year.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3) ${MENU} Which industry has the most number of Data Scientist positions?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4) ${MENU} Which top 5 employers file the most petitions each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6) ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7) ${MENU} Create a bar graph to depict the number of applications for each year${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8) ${MENU}Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9) ${MENU} Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10) ${MENU} Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11) ${MENU}Export result for option no 10 to MySQL database.${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[0;37m' # bold red
    RESET='\033[0;37m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}
clear
show_menu
	while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1a) clear;
        option_picked "1 a) Is the number of petitions with Data Engineer job title increasing over time?";

	hadoop jar Qs1a.jar Qs1a /user/hive/warehouse/ramit.db/h1b_final /h1b/outputQ1a
           
          hadoop fs -cat /h1b/outputQ1a/p*

        show_menu;
        ;;
        1b) clear;
        option_picked "1 b) Find top 5 job titles who are having highest average growth in applications. ";

       	pig -x local H1BProject1b.pig

        show_menu;
        ;;  
        2a) clear;
        option_picked "2 a) Which part of the US has the most Data Engineer jobs for each year?";

        echo -e "Enter  the Year (2011,2012,2013,2014,2015,2016)"
        
         read var
         
        hive -e "select worksite,COUNT(worksite) as ws from ramit.h1b_final where job_title LIKE '%DATA ENGINEER%' and year=$var group by worksite   order by ws desc limit 1;"
        show_menu;
        ;;
	    2b) clear;
        option_picked "2 b) find top 5 locations in the US who have got certified visa for each year.";
        echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"

	read var

	    hive -e "select worksite,COUNT(worksite) as w from ramit.h1b_final where case_status='CERTIFIED' and year=$var group by worksite order by w desc limit 5;"  
        show_menu;
        ;;  
	    3) clear;
        option_picked "3) Which industry has the most number of Data Scientist positions?";
        	pig -x local H1BProject3.pig
        show_menu;
        ;;
        4) clear;
        option_picked "4)Which top 5 employers file the most petitions each year?";
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
                   
     read var

       hive -e "select employer_name,COUNT(year) as tot from ramit.h1b_final where year=$var group by employer_name  order by tot desc limit 5;"
		
        show_menu;
        ;;
        5) clear;
        option_picked "5) Find the most popular top 10 job positions for H1B visa applications for each year?";
	 
          echo -e "$ ${MENU}Select option :${NORMAL}"
   
         echo -e "${MENU}**${NUMBER} 1) ${MENU}For All Case Status${NORMAL}"
         echo -e "${MENU}**${NUMBER} 2) ${MENU} For Certified Only${NORMAL}"

         read n
        case $n in 
        1) echo "For All"

       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"

        read year

         pig -x local -p whichyear=$year -f H1BProject5a.pig	

        ;;
       
       2) echo "For Certified Only"

       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"

        read year

       hive -e "select job_title,COUNT(job_title) as tot from ramit.h1b_final where case_status='CERTIFIED' and year=$year group by job_title order by    tot desc limit 10;"

        ;;
       *) echo "Select either option 1 or 2";;
         
         esac 

        show_menu;
        ;;

        6) clear;
       	
	option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting 		the pattern of All the cases over the period of time.";
		
          pig -x local  H1BProject6.pig

        show_menu;
        ;;

	7) clear;

        option_picked "7) Create a bar graph to depict the number of applications for each year";
		
            hadoop jar Qs7.jar Qs7 /user/hive/warehouse/ramit.db/h1b_final /h1b/outputQ7
           
          hadoop fs -cat /h1b/outputQ7/p*

        show_menu;
        ;;
		8) clear;
        option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order";
		
       echo -e "$ ${MENU}Select option :${NORMAL}"
   
         echo -e "${MENU}**${NUMBER} 1) ${MENU}For Full Time (Certified)${NORMAL}"
         echo -e "${MENU}**${NUMBER} 2) ${MENU} For Part Time (Certified)${NORMAL}"

         read n
        case $n in 
        1) echo "For Full Time"

       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"

        read year

         pig -x local -p whichyear=$year -f H1BProject8.pig	

        ;;
       
       2) echo "For Part Time"

       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"

        read year

         pig -x local -p whichyear=$year -f H1BProject8b.pig	

        ;;
       *) echo "Select either option 1 or 2";;
         
         esac 

        show_menu;
        ;;
		9) clear;
		option_picked "9) Which are   employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?"

         hadoop jar Qs9.jar Qs9 /user/hive/warehouse/ramit.db/h1b_final /h1b/outputQ9
           
          hadoop fs -cat /h1b/outputQ9/p*

           show_menu;
            ;;

		10) clear;

		option_picked "10) Which are the job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000?"
		
	hadoop jar Qs10.jar Qs10 /user/hive/warehouse/ramit.db/h1b_final /h1b/outputQ10
           
         hadoop fs -cat /h1b/outputQ10/p*

         show_menu;
         ;;
		11) clear;
		option_picked "11) Export result for question no 10 to MySql database."
		
          sqoop export --connect jdbc:mysql://localhost/Q11 --username hduser --password 'password' --table Q11 --update-mode  allowinsert --export-dir /home/hduser/Downloads/Q10 --input-fields-terminated-by '\t' ;

        show_menu;
        ;;
		\n) exit;   
		;;
        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi
done
