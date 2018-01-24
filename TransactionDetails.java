import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

public class TransactionDetails {

	public static void main(String[] args)
	{
		try
		{ 
			FileWriter fw=new FileWriter("E:\\Program\\TransactionDetails.txt");
			BufferedWriter bw=new BufferedWriter(fw);
			FileReader fr=new FileReader("E:\\Program\\TransactionDetails.txt");
			BufferedReader br=new BufferedReader(fr);
			ArrayList<String> al= new ArrayList<>();
			Scanner scn=new Scanner(System.in);
			String customerID,productID,dateOfPurchase,productType;
			String str,cID="";
			int productPrice,quantity,totalPrice,discountPercentage;
			int j,count=0,x=0;
			System.out.println("Enter number of Transaction details");
			int n=scn.nextInt();
			scn.nextLine();
			//Writing data to file 
			for(int i=0;i<n;i++)
			{
				System.out.println("Enter Customer ID");
				customerID=scn.nextLine();
				bw.write(customerID);
				bw.append(",");
				System.out.println("Enter Product ID");
				productID=scn.nextLine();
				bw.write(productID);
				bw.append(",");
				System.out.println("Enter Date Of Purchase");
				dateOfPurchase=scn.nextLine();
				bw.write(dateOfPurchase);
				bw.append(",");
				System.out.println("Enter Product Type");
				productType=scn.nextLine();
				bw.write(productType);
				bw.append(",");
				System.out.println("Enter Product Price");
				productPrice=scn.nextInt();
				scn.nextLine();
				bw.write(new Integer(productPrice).toString());
				bw.append(",");
				System.out.println("Enter Quantity");
				quantity=scn.nextInt();
				scn.nextLine();
				bw.write(new Integer(quantity).toString());
				bw.append(",");
				System.out.println("Enter Total Price");
				totalPrice=quantity*productPrice;
				System.out.println(totalPrice);
				bw.write(new Integer(totalPrice).toString());
				bw.append(",");
				System.out.println("Enter Discount Percentage");
				discountPercentage=scn.nextInt();
				scn.nextLine();
				bw.write(new Integer(discountPercentage).toString());
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
			// To Display the details of all products where discount is>25%.
			for(j=0;j<n;j++)
			{
				int discount=Integer.parseInt(al.get((8*j)+7));
				if(discount>25) {
				System.out.println("ProductID:"+(al.get((8*j)+1))+" Product Price:"+(al.get((8*j)+4))+" Product Type:"+(al.get((8*j)+3))+" Discount Percentage:"+(al.get((8*j)+7))+"%");
				}
			}
			//Customer did highest purchase in a day
			String date=al.get(2);
			for(j=0;j<n;j++)
			{
				String cDate=al.get((8*j)+2);
				if(cDate.equals(date))
				{
				int purchase=Integer.parseInt(al.get((8*j)+6));
				if(purchase>x) {
				x=purchase;
				cID=al.get(8*j);
				}
				}
			}
			System.out.println("Highest purchase made by customer:"+cID+" is "+x);
			//Total Purchase made by customer in one year
			
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}
	
	
	
	
}
