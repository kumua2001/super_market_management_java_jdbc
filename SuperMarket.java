package dummy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Date;
import java.util.Scanner;
import java.util.StringTokenizer;

public class SuperMarket {

	public enum Command {
		CREATE, WRITE, ADD, UPDATE, REMOVE, STATUS, HISTORY, TOTAL, CUSTOMER, CUSTOMERS, INVALID_COMMAND;

		public static Command toStr(String str) {
			try {
				return valueOf(str);
			} catch (Exception ex) {
				return INVALID_COMMAND;
			}
		}

	}

	public static double total;
	public static String productName;
	public static double productCost;
	public static int productCount;

	public static void main(String args[]) throws SQLException {

		SuperMarket superMarket = new SuperMarket();
		String activeCustomer = null;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st;
		String query = "";
		String[] input;
		Date now = new Date();
		Timestamp currentDate = new Timestamp(now.getTime());
		try {
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/super_market", "root", "");
			Statement stmt = con.createStatement();
			String init = "create table if not exists customers(customerName varchar(30) not null,mobileNumber varchar(20),\r\n"
					+ "joinDate timestamp, primary key(customerName,mobileNumber));";
			stmt.execute(init);
			init = "create table if not exists products(productName varchar(30) not null unique,productKey varchar(10) unique,\r\n"
					+ "productCount int, productCost double, primary key(productName,productKey));";
			stmt.execute(init);
			init = "create table if not exists transactions(transactionDate timestamp,cutomerName varchar(30),\r\n"
					+ "productName varchar(30),productCount int, productCostperUnit double,productAmount double);";
			stmt.execute(init);

			System.out.println(" Create a new customer using the command ' create [Name] [Mobile Number] '");

			while (true) {
				try {
					query = br.readLine();
					if (query.equalsIgnoreCase("quit"))
						break;
				} catch (IOException e) {
					e.printStackTrace();
				}

				st = new StringTokenizer(query);
				input = new String[st.countTokens()];
				int i = 0;
				while (st.hasMoreTokens()) {
					String token = st.nextToken();
					input[i] = token;
					i++;
				}

				switch (Command.toStr(input[0].toUpperCase())) {

				case CREATE: {
					// Creating Customer account
					if (input.length == 3) {
						String customerName = input[1];
						String mobileNumber = input[2];

						try {
							superMarket.addCustomer(con, customerName, mobileNumber, currentDate);
							activeCustomer = customerName;
							total = 0;
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (SQLIntegrityConstraintViolationException e) {
							activeCustomer = customerName;
							System.out.println(" Customer account aldready created ");
							total = 0;
						} catch (SQLException e) {
							e.printStackTrace();
						}

					} else
						System.out.println(
								" COMMAND ERROR : Please use the Command' create [name] [Mobile Number]' to create a new user");
					break;
				}

				case WRITE: {
					// Buying Products
					if (input.length == 3) {
						String proKey = input[1];
						int proCount = Integer.parseInt(input[2]);

						if (activeCustomer == null) {
							System.out.println(
									" ERROR : Please Create customer account ' create [name] [Mobile Number]' ");
							break;
						}
						productName = null;
						productCost = 0;

						String productNameSql = "select * from products where productKey=?;";
						PreparedStatement pstmt1 = con.prepareStatement(productNameSql);
						pstmt1.setString(1, proKey);
						ResultSet rst = pstmt1.executeQuery();

						while (rst.next()) {
							productName = rst.getString(1);
							productCount = rst.getInt(3);
							productCost = rst.getDouble(4);

						}
						if (productName != null) {
							if (proCount > 0) {
								if (proCount <= productCount) {

									// Insert to transactions Table
									String sql = "insert into transactions values(?,?,?,?,?,?);";
									PreparedStatement pstmt = con.prepareStatement(sql);
									pstmt.setTimestamp(1, currentDate);
									pstmt.setString(2, activeCustomer);
									pstmt.setString(3, productName);
									pstmt.setInt(4, proCount);
									pstmt.setDouble(5, productCost);
									pstmt.setDouble(6, (proCount * productCost));
									pstmt.execute();
									// Update Product Count
									String changeProductCountSql = "update products set productCount=? where productName=?;";
									PreparedStatement pstmt2 = con.prepareStatement(changeProductCountSql);
									pstmt2.setInt(1, (productCount - proCount));
									pstmt2.setString(2, productName);
									pstmt2.execute();
									// add buying amount to total
									total = total + (productCost * proCount);
									System.out.println(" " + productName + "  ->  " + proCount + " * " + productCost
											+ " = " + (productCost * proCount));

									productName = null;
									pstmt.close();
									rst.close();

								} else
									System.out.println(" ERROR : You do not have sufficient product ");
							} else
								System.out.println(" ERROR : Invalid Product Count");
						} else
							System.out.println(" ERROR : Invalid Product Key");
					} else
						System.out.println(
								" COMMAND ERROR : Please use the command 'write [Product Name] [Count]' to buy the product");
					break;
				}

				case ADD: {
					// Adding Products
					if (input.length == 5) {

						String productName = input[1];
						String productKey = input[2];
						int productCount = Integer.parseInt(input[3]);
						double productCost = Double.parseDouble(input[4]);
						superMarket.addProduct(con, productName, productKey, productCost, productCount);

					} else
						System.out.println(
								" COMMAND ERROR : Please use the command 'add [product name] [product key] [count] [amount] ' to add a new product ");
					break;
				}

				case UPDATE: {
					// Updating Products
					if (input.length == 5) {

						String proName = input[1];
						String proKey = input[2];
						int proCount = Integer.parseInt(input[3]);
						double proCost = Double.parseDouble(input[4]);
						superMarket.updateProduct(con, proName, proKey, proCount, proCost);

					} else
						System.out.println(
								" COMMAND ERROR : Please use the command 'update [product name] [product key] [count] [amount]' to modify product details ");
					break;
				}

				case REMOVE: { // Removing Products
					if (input.length == 2) {
						String productName = input[1];
						superMarket.removeProduct(con, productName);

					} else
						System.out.println(
								" COMMAND ERROR : Please use the command ' remove [product name] '  to remove a product ");
					break;
				}

				case HISTORY: {
					// Viewing all transaction history
					if (input.length == 2) {
						String givenDate = input[1];
						ResultSet rs = superMarket.historyMethod(con, givenDate);

						if (rs != null) {
							System.out.println(
									"--------------------------+-----------------+------------------+------------+--------------+-----------------+");
							System.out.println(
									"           DATE           |  CUSTOMER NAME  |   PRODUCT NAME   |   COUNT    |    AMOUNT    |   TOTAL AMOUNT  |");
							System.out.println(
									"--------------------------+-----------------+------------------+------------+--------------+-----------------+");

							while (rs.next()) {
								Timestamp transactionDate = rs.getTimestamp(1);
								String customerName = rs.getString(2);
								String productName = rs.getString(3);
								int productCount = rs.getInt(4);
								double productCost = rs.getDouble(5);
								double productAmount = rs.getDouble(6);

								System.out.format("%11s", transactionDate);
								System.out.print("     |  ");
								System.out.format("%12s", customerName);
								System.out.print("   |  ");
								System.out.format("%12s", productName);
								System.out.print("    |  ");
								System.out.format("%6d", productCount);
								System.out.print("    |  ");
								System.out.format("%8s", superMarket.toString(productCost));
								System.out.print("    |  ");
								System.out.format("%12s", superMarket.toString(productAmount));
								System.out.print("   |  ");
								System.out.println(
										"\n--------------------------+-----------------+------------------+------------+--------------+-----------------+");

							}
						}

					} else
						System.out.println(
								" COMMAND ERROR : Please use the command ' [history] { today / yesterday / lastweek / lastmonth / custom / all }' to get transactions");
					break;
				}

				case STATUS: {
					// Viewing Product Details
					if (input.length == 1) {

						String sql = "select * from products ;";
						
						ResultSet rs = stmt.executeQuery(sql);

						System.out.println(" ---------------+---------------+------------+--------------+");
						System.out.println("  PRODUCT NAME  |  PRODUCT KEY  |   COUNT    |     COST     |");
						System.out.println(" ---------------+---------------+------------+--------------+");

						while (rs.next()) {
							String productName = rs.getString(1);
							String productKey = rs.getString(2);
							int productCount = rs.getInt(3);
							double productCost = rs.getDouble(4);

							System.out.format("%12s", productName);
							System.out.print("    |  ");
							System.out.format("%9s", productKey);
							System.out.print("    |  ");
							System.out.format("%6d", productCount);
							System.out.print("    |  ");
							System.out.format("%8s", superMarket.toString(productCost));
							System.out.print("    |  ");
							System.out.println("\n ---------------+---------------+------------+--------------+");
						}
						rs.close();
						

					} else
						System.out.println(
								"COMMAND ERROR : Please use the command ' [status] '  to get Product details ");
					break;
				}

				case TOTAL: {
					// Viewing current total
					if (input.length == 1) {
						try {
							System.out.println(" Total = " + total);
						} catch (Exception e) {
							System.out.println(" Customer not created");
						}
					} else
						System.out.println(" COMMAND ERROR : Please use the command ' [total] ' to get total amount ");

					break;
				}

				case CUSTOMER: {
					// Viewing particular Customer transactions
					if (input.length == 2) {
						String customerName = input[1];

						String sql = "select * from transactions where customerName=? order by transactionDate ;";
						PreparedStatement pstmt = con.prepareStatement(sql);
						pstmt.setString(1, customerName);
						ResultSet rs = pstmt.executeQuery();

						System.out.println(
								"--------------------------+-----------------+------------------+------------+--------------+-----------------+");
						System.out.println(
								"           DATE           |  CUSTOMER NAME  |   PRODUCT NAME   |   COUNT    |     COST     |   TOTAL AMOUNT  |");
						System.out.println(
								"--------------------------+-----------------+------------------+------------+--------------+-----------------+");

						while (rs.next()) {
							Timestamp transactionDate = rs.getTimestamp(1);
							String customerName1 = rs.getString(2);
							String productName = rs.getString(3);
							int productCount = rs.getInt(4);
							double productCost = rs.getDouble(5);
							double productAmount = rs.getDouble(6);

							System.out.format("%11s", transactionDate);
							System.out.print("     |  ");
							System.out.format("%12s", customerName1);
							System.out.print("   |  ");
							System.out.format("%12s", productName);
							System.out.print("    |  ");
							System.out.format("%6d", productCount);
							System.out.print("    |  ");
							System.out.format("%8s", superMarket.toString(productCost));
							System.out.print("    |  ");
							System.out.format("%12s", superMarket.toString(productAmount));
							System.out.print("   |  ");
							System.out.println(
									"\n--------------------------+-----------------+------------------+------------+--------------+-----------------+");

						}

					} else
						System.out.println(
								" COMMAND ERROR : Please use the command ' customer [customer name] ' to get transaction history for particular customer");
					break;
				}

				case CUSTOMERS: {
					// Viewing all customer Details
					if (input.length == 1) {

						String sql = "select * from customers order by joinDate;";
						
						ResultSet rs = stmt.executeQuery(sql);

						System.out.println(" ---------------+-----------------+--------------------------+");
						System.out.println(" CUSTOMER NAME  |  MOBILE NUMBER  |        JOIN DATE         |");
						System.out.println(" ---------------+-----------------+--------------------------+");
						while (rs.next()) {
							String customerName = rs.getString(1);
							String mobileNumber = rs.getString(2);
							Timestamp joinDate = rs.getTimestamp(3);

							System.out.format("%12s", customerName);
							System.out.print("    |  ");
							System.out.format("%11s", mobileNumber);
							System.out.print("    |  ");
							System.out.format("%8s", joinDate);
							System.out.print("   |  ");
							System.out.println("\n ---------------+-----------------+--------------------------+");

						}
						rs.close();
						

					} else
						System.out.println(
								" COMMAND ERROR : Please use the command ' [customers] '  to get all customer details ");
					break;
				}

				case INVALID_COMMAND: {
					System.out.println(
							" INVALID COMMAND: commands - CREATE, WRITE, ADD, UPDATE, REMOVE, STATUS, HISTORY, TOTAL, CUSTOMER, CUSTOMERS ");
					break;
				}

				}

			}
		} catch (SQLSyntaxErrorException e) {
			System.out.println(" super_market database not exist. so please ' create database as super_market ' ");
			System.exit(1);

		}

	}

	@SuppressWarnings("resource")
	public ResultSet historyMethod(Connection con, String givenDate) throws SQLException {

		LocalDate today = LocalDate.now();
		LocalDate yesterday = today.plusDays(-1);
		LocalDate tomorrow = today.plusDays(1);
		LocalDate lastWeek = today.plusDays(-7);
		LocalDate lastMonth = today.plusMonths(-1);
		Scanner scan = new Scanner(System.in);
		String sql = "select * from transactions where (transactionDate>=? and transactionDate<=?) order by transactionDate  ;";
		PreparedStatement pstmt1 = con.prepareStatement(sql);
		switch (givenDate) {
		case "today": {
			pstmt1.setString(1, today.toString());
			pstmt1.setString(2, tomorrow.toString());
			break;
		}
		case "yesterday": {
			pstmt1.setString(1, yesterday.toString());
			pstmt1.setString(2, today.toString());
			break;
		}
		case "lastweek": {
			pstmt1.setString(1, lastWeek.toString());
			pstmt1.setString(2, tomorrow.toString());
			break;
		}
		case "lastmonth": {
			pstmt1.setString(1, lastMonth.toString());
			pstmt1.setString(2, tomorrow.toString());
			break;
		}
		case "custom": {
			try {
				System.out.println(" Enter From date : 'yyyy-MM-dd' ");
				String fromDate = scan.next();
				LocalDate fDate = LocalDate.parse(fromDate);
				System.out.println(" Enter To date : 'yyyy-MM-dd' ");
				String toDate = scan.next();
				LocalDate tDate = LocalDate.parse(toDate);

				if (fDate.isBefore(tDate)) {
					pstmt1.setString(1, fromDate);
					pstmt1.setString(2, toDate);
				} else {
					pstmt1.setString(1, toDate);
					pstmt1.setString(2, fromDate);
				}
			} catch (DateTimeParseException | SQLException ex) {
				System.out.println(" Invalid Date format");
				return null;
			}
			break;
		}
		case "all":
			sql = "select * from transactions order by transactionDate  ;";
			pstmt1 = con.prepareStatement(sql);
		default:
			System.out.println(" ERROR : Invalid Command");
		}

		ResultSet rs = pstmt1.executeQuery();
		return rs;

	}

	public void addCustomer(Connection con, String customerName, String mobileNumber, Timestamp joinDate)
			throws ClassNotFoundException, SQLException {

		String sql = "insert into customers values(?,?,?);";
		PreparedStatement pstmt = con.prepareStatement(sql);
		pstmt.setString(1, customerName);
		pstmt.setString(2, mobileNumber);
		pstmt.setTimestamp(3, joinDate);
		pstmt.execute();
		System.out.println(" Customer added successfully");
		pstmt.close();
	

	}

	public void updateProduct(Connection con, String proName, String proKey, int proCount, double proCost)
			throws SQLException {

		productName = null;
		productCost = 0;
		String productNameSql = "select * from products where productName=?;";
		PreparedStatement pstmt1 = con.prepareStatement(productNameSql);
		pstmt1.setString(1, proName);
		ResultSet rst = pstmt1.executeQuery();

		while (rst.next()) {
			productName = rst.getString(1);
			productCost = rst.getDouble(4);
		}
		if (proCost == 0) {
			// your not wish to change product cost , assign '0'
			proCost = productCost;
		}
		if (productName != null) {
			if (proCount > 0 && proCost > 0) {

				String sql = "update products set productKey=?,productCount=?,productCost=?  where productName=?;";
				PreparedStatement pstmt = con.prepareStatement(sql);
				pstmt.setString(1, proKey);
				pstmt.setInt(2, proCount);
				pstmt.setDouble(3, proCost);
				pstmt.setString(4, proName);
				pstmt.execute();
				System.out.println(" Product updated successfully");
				pstmt.close();

			} else
				System.out.println(" ERROR : Invalid product count or product cost");
		} else
			System.out.println(" ERROR : Product name not found . please check product status");

	}

	public void removeProduct(Connection con, String proName) throws SQLException {

		productName = null;
		String productNameSql = "select * from products where productName=?;";
		PreparedStatement pstmt1 = con.prepareStatement(productNameSql);
		pstmt1.setString(1, proName);
		ResultSet rst = pstmt1.executeQuery();

		while (rst.next()) {
			productName = rst.getString(1);
		}
		if (productName != null) {

			String sql = "delete from products where productName=?;";
			PreparedStatement pstmt = con.prepareStatement(sql);
			pstmt.setString(1, productName);
			pstmt.execute();
			System.out.println(" Product removed successfully");
			pstmt.close();

		} else
			System.out.println(" ERROR : Invalid Product Name ");
	}

	public void addProduct(Connection con, String productName, String productKey, double productCost,
			int productCount) {
		try {

			String sql = "insert into products values(?,?,?,?);";
			PreparedStatement pstmt = con.prepareStatement(sql);
			pstmt.setString(1, productName);
			pstmt.setString(2, productKey);
			pstmt.setInt(3, productCount);
			pstmt.setDouble(4, productCost);
			pstmt.execute();
			System.out.println(" Product added successfully");
			pstmt.close();

		}

		catch (SQLIntegrityConstraintViolationException e) {
			System.out.println(" This Product Name is aldready added");
		}

		catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String toString(double productCost) {
		return String.valueOf(productCost);
	}
}
