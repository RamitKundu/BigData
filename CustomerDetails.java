import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

public class CustomerDetails {
	
	public static void main(String[] args)
	{
		try
		{ 	
			FileWriter fw=new FileWriter("E:\\Program\\CustomerDetails.txt");
			BufferedWriter bw=new BufferedWriter(fw);
			FileReader fr=new FileReader("E:\\Program\\CustomerDetails.txt");
			BufferedReader br=new BufferedReader(fr);
			ArrayList<String> al= new ArrayList<>();
			Scanner scn=new Scanner(System.in);
			String customerID,customerName,customerGender,customerAddress,customerCity,customerCountry,customerDOB,customerPhone;
			String str;
			int customerAge,customerPin;
			int j,maleCount=0,femaleCount=0,ageCount=0,phoneCount=0,countryCount=0,otherCountryCount=0;
			System.out.println("Enter number of Customer details");
			int n=scn.nextInt();
			scn.nextLine();
			//Writing data to file 
			for(int i=0;i<n;i++)
			{
				System.out.println("Enter Customer ID");
				customerID=scn.nextLine();
				bw.write(customerID);
				bw.append(",");
				System.out.println("Enter Customer Name");
				customerName=scn.nextLine();
				bw.write(customerName);
				bw.append(",");
				System.out.println("Enter Customer Gender");
				customerGender=scn.nextLine();
				bw.write(customerGender);
				bw.append(",");
				System.out.println("Enter Customer Address");
				customerAddress=scn.nextLine();
				bw.write(customerAddress);
				bw.append(",");
				System.out.println("Enter Customer City");
				customerCity=scn.nextLine();
				bw.write(customerCity);
				bw.append(",");
				System.out.println("Enter Customer Country");
				customerCountry=scn.nextLine();
				bw.write(customerCountry);
				bw.append(",");
				System.out.println("Enter Customer DOB");
				customerDOB=scn.nextLine();
				bw.write(customerDOB);
				bw.append(",");
				System.out.println("Enter Customer Age");
				customerAge=scn.nextInt();
				scn.nextLine();
				bw.write(new Integer(customerAge).toString());
				bw.append(",");
				System.out.println("Enter Customer Pin");
				customerPin=scn.nextInt();
				scn.nextLine();
				bw.write(new Integer(customerPin).toString());
				bw.append(",");
				System.out.println("Enter Customer Phone");
				customerPhone=scn.nextLine();
				bw.write(customerPhone);
				bw.newLine();
			}
			bw.close();
			fw.close();
			System.out.println("Written Successfully");
			//Reading data from file
			while((str=br.readLine())!=null)
			{
				StringTokenizer st=new StringTokenizer(str,",");
				while(st.hasMoreTokens()) {
				al.add(st.nextToken());
				}
			}
			//To Display Total Male and Female customer
			for(j=0;j<n;j++)
			{
				String gender=al.get((10*j)+2);
				if(gender.equalsIgnoreCase("Male")) {
				maleCount++;
				}
				if(gender.equalsIgnoreCase("Female")) {
				femaleCount++;
				}
			}
			System.out.println("Total Male Customer:"+maleCount);
			System.out.println("Total Female Customer:"+femaleCount);
			//To Display total customer whose age is greater than 18
			for(j=0;j<n;j++)
			{
				int age=Integer.parseInt(al.get((10*j)+7));
				if(age>18) {
				ageCount++;
				}
			}
			System.out.println("Total customer whose age>18:"+ageCount);
			//Display total customer country wise
			for(j=0;j<n;j++)
			{
				String country=al.get((10*j)+5);
				if(country.equalsIgnoreCase("India")) {
				countryCount++;
				}
				else {
				otherCountryCount++;
				}
			}
			System.out.println("Total Customers in India:"+countryCount);
			System.out.println("Total Customers in Other countries:"+otherCountryCount);
			//Display total customer who does not have phone number
			for(j=0;j<n;j++)
			{
				String phone=al.get((10*j)+9);
				if(phone.equalsIgnoreCase("null")) {
				phoneCount++;
				}
			}
			System.out.println("Total customer who don't have phone:"+phoneCount);
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}

	
	
	

}
