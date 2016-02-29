package qosdbc.allocation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBConnection {

	private Connection connection = null;

	public Connection connect(String addres, String DBname, String port ,String user, String password ){
		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			e.printStackTrace();
			return null;

		}		

		try {
			String con = "jdbc:postgresql://"+ addres +":"+port+"/"+ DBname;
			System.out.println(con);

			connection = DriverManager.getConnection(
					con, user,
					password);

		} catch (SQLException e1) {

			e1.printStackTrace();
			return null;

		}
		System.out.println("Conexão com o banco ativa!");
		return connection;
		
	}
	

}
