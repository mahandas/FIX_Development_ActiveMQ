# FIX Development using ActiveMQ
  
Contains code for Developing FIX applications(Financial information exchange) using MessageBroker Architecture. 
Let's see what i mean by this.
  
# What's FIX protocol?
  
The FIX (Financial Information Exchange) Protocol has been around for more than 25 years, and helped restore a broken financial trading system by implementing across-the-board information flow. 
  
FIX Protocols are the gold standard for financial service entities. It is used by both buyers and sellers in the financial space. These range from brokers, banks, mutual funds, stock exchanges, and dealers. The use cases for FIX are: you can trade just about anything. Equity, bonds, derivatives — you name it, it’s likely being traded over FIX.
  
FIX has a certain layout. FIX works by defining preset “tags” as value placeholders. Orders are entered by defining variables in the tags — for example the value for “side” of the trade can be a 1 for Buy or 2 for Sell, in Tag 54. The biggest benefit of FIX is that all of the information is standardized, so an order made on one end of the world will be easily received and executed by the other half without any issue.
  
FIX is often used when you want to connect directly with a liquidity provider, i.e. a buyer. Clients will also use FIX if they want to make sure that their systems are anonymous and protected from disclosing private information.
  
# Architecture 

Liquidity provider <=> FIX Application (JAVA) <=> ActiveMQ <=> Web Application(.NET)  
                                              <=> DataBase <=>  
                                           
* <=> represents To:from data transfer between the entities
* Liquidity provider can be investment banks like JPM, Goldman Sachs, UBS, OCBC.
* FIX application is the code that is provided here
  
The Liquidity provider is the one that provides prices of a product and books the order. In case of trading platforms, the FIX application connects to the IP and port provided by these liquidity providers. Once the connection is established, Heart beat messages are exchanged between the two applications. These Hearbeat messages are constantly transferred to check the health of the application. 
  
Data from the webapplication to the Liquidity provider and vice versa is transferred via FIX application. The transmission occurs at the TCP/IP level making it Fast and light weight. Once the Data from is in the application, it is parsed into internal structure and transmitted to the webapplication. This transmission of data occurs via Message Brokers(namely Active MQ). Active MQ allows Queing of data preventing loss of data. It also allows dumping data into the Data base server for Audit purposes.
  
The Webapplication and the FIX application are in a pub-sub architecture with the ActiveMQ. This enables data to be consumed on arrival and in case of latency in processing, the data is queued into the message broker. Data in the webapplication is then pushed to the client page using websockets. But that code is placed in a  different repository.

# The Flow of Code 

The code starts with the main method (in FXClient.java). Creates an object of the class it resides in and calls the methods.  
'''java
public static void main(String[] args) {
		FXClient client = new FXClient();
		client.initializeParams(args);
		client.startClient();
	}
'''

# references
1. https://medium.com/xtrd/a-look-into-fix-protocols-72ec15868e65
2. 
