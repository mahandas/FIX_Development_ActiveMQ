package client;


import client.FIX_MessageStructure.StructNewOrderSingle;
import client.FIX_MessageStructure.StructQuoteRequest;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import org.json.JSONException;
import org.json.JSONObject;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.field.CFICode;
import quickfix.field.ClOrdID;
import quickfix.field.Currency;
import quickfix.field.FutSettDate;
import quickfix.field.FutSettDate2;
import quickfix.field.HandlInst;
import quickfix.field.NoPartyIDs;
import quickfix.field.NoRelatedSym;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.OrderQty2;
import quickfix.field.PartyID;
import quickfix.field.PartyIDSource;
import quickfix.field.PartyRole;
import quickfix.field.Price;
import quickfix.field.QuoteID;
import quickfix.field.QuoteReqID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix44.NewOrderSingle;
import quickfix.fix44.QuoteRequest;
import util.DateUtil;


public class RequestQuote implements MessageListener{

	public RequestQuote() {

	}

	private static final Logger LOG = Logger.getLogger(RequestQuote.class.getName());
	private static final Properties config = ConfigReader.getConfigFile();


	@Override
	public void onMessage(Message message) {

		int pollInterval = 1000;
		try {
			pollInterval = Integer.parseInt(config.getProperty("Poll_Interval"));
		} catch (Exception e) {
			LOG.info("Loaded default Poll Interval :" + pollInterval + "ms");
		}
		MapMessage crs1=(MapMessage) message;

		JSONObject crs;
		try {
			//			System.out.println(crs1.getText());
			crs = new JSONObject(crs1.getStringProperty("DATA"));
			if(crs1.getStringProperty("MSGTYPE").equalsIgnoreCase("REQUEST"))
			{	System.out.println("Placing for ID"+crs.getString("Request_ID"));
				sendRequestForQuotation(crs);
			}
			else if(crs1.getStringProperty("MSGTYPE").equalsIgnoreCase("ORDER"))
			{	System.out.println("Placing Order for Order ID"+crs.getString("Order_ID"));
				placeOrder(crs);
			}
			else
				System.out.println("Invalid Message Type");
		} catch (JSONException | JMSException e1) {

			e1.printStackTrace();
		}
	}
	
	public void sendRequestForQuotation(JSONObject crs) {


		StructQuoteRequest objQuoteRequest = new StructQuoteRequest();

		try {            

			objQuoteRequest.setQuoteReqID(crs.getString("Request_ID")) ;

			objQuoteRequest.setNoRelatedSym(1);	 


			objQuoteRequest.setSymbol(crs.getString("Deal_Pair")) ;  //should be CCY1/CCY2 

			objQuoteRequest.setSecurityID(objQuoteRequest.getSymbol());
			objQuoteRequest.setCurrency(crs.getString("Quoted_Ccy"));

			if(crs.getString("Deal_Type").compareToIgnoreCase("SPOT")==0)
				objQuoteRequest.setCFICode("SPOT") ; 
			else if(crs.getString("FR_Deal_Type").compareToIgnoreCase("OUTRIGHT")==0)
			{	
				objQuoteRequest.setCFICode("OUTRIGHT");
				
			}
			objQuoteRequest.setOrderQty(crs.getDouble("Quoted_Amt"));
			objQuoteRequest.setExpireTime(DateUtil.getCurrentDateTime_Str());

			try {
				buildQuoteRequestAndSend(objQuoteRequest);
			} catch (Exception e) {
				e.printStackTrace();
				LOG.severe("Error in buildQuoteRequestAndSend for ID : " + objQuoteRequest.getQuoteReqID() +
						" : " + e.getMessage());
			}
		} catch (Exception e) {
			LOG.severe("Error in sendRequestForQuotation for ID : " + objQuoteRequest.getQuoteReqID() +
					" : " + e.getMessage());

		} finally {
			objQuoteRequest = null;

		}




	}

	private void buildQuoteRequestAndSend(StructQuoteRequest objQuoteRequest) throws Exception {

		if (!ApplicationImpl.isLoggedOn()) {
			throw new Exception("Unable get logon response from FIX server..");
		}

		QuoteRequest quoteRequest = new QuoteRequest();
		quoteRequest.set(new QuoteReqID(objQuoteRequest.getQuoteReqID()));		//Set QuoteReqID

		NewOrderSingle.NoPartyIDs grpPartyID = new NewOrderSingle.NoPartyIDs();
		grpPartyID.setField(new PartyIDSource('D'));			
		grpPartyID.setField(new PartyRole(13));				
		quoteRequest.addGroup(grpPartyID);

		quoteRequest.set(new NoRelatedSym(1));
		QuoteRequest.NoRelatedSym noRelatedSym = new QuoteRequest.NoRelatedSym();
		noRelatedSym.set(new Symbol(objQuoteRequest.getSymbol())); 
		noRelatedSym.set(new CFICode(objQuoteRequest.getCFICode()));
		quoteRequest.addGroup(noRelatedSym);

		quoteRequest.setDouble(38,objQuoteRequest.getOrderQty());


		quoteRequest.setString(15,objQuoteRequest.getCurrency());
		if(config.getProperty("Streaming").equalsIgnoreCase("Y"))
			quoteRequest.setString(126,objQuoteRequest.getExpireTime());
	

		SessionID sessionID = FXClient.getSessionID();
		if (sessionID != null && Session.doesSessionExist(sessionID)) {
			if (Session.sendToTarget(quoteRequest, sessionID)) { 
				LOG.info( "Quote request sent successfully for ID :" + objQuoteRequest.getQuoteReqID() );
			} else {
				throw new Exception("Error while sending quote.");
			}
		} else {
			throw new Exception("Session does not exist ... sessionId : "+sessionID);
		}
	}
	
	public void placeOrder(JSONObject crs) {


		StructNewOrderSingle objNewOrder = new StructNewOrderSingle();
		SessionID sessionID;

		try {

			if (!ApplicationImpl.isLoggedOn()) {
				throw new Exception("Unable get logon response from FX server..");
			}
			objNewOrder.setClOrdID(crs.getString("Order_ID"));
			objNewOrder.setQuoteID(crs.getString("Quote_ID")); 

			objNewOrder.setSymbol(crs.getString("Symbol"));
			objNewOrder.setOrderQty(crs.getDouble("OrderQty"));
			objNewOrder.setSide((crs.getString("Side")).charAt(0)=='1'?Side.BUY:Side.SELL);
			objNewOrder.setPrice(crs.getDouble("Price"));
			System.out.println("Booking Price:"+objNewOrder.getPrice());
			objNewOrder.setCurrency(crs.getString("Currency"));
			sessionID = UBSFXClient.getSessionID();
			if (sessionID != null && Session.doesSessionExist(sessionID)) {
				if (Session.sendToTarget(buildOrder(objNewOrder), sessionID) == true) {
					LOG.info("Order request sent successfully For Order No:" + objNewOrder.getClOrdID() + "");
				} else {
					throw new Exception("Error while sending Order");
				}
			} else {
				throw new Exception("Session does not exist ...");
			}
		} catch (Exception e) {
			LOG.severe("Error in Order No : " + objNewOrder.getClOrdID() + " : " + e.getMessage());
		} 


	}
	private NewOrderSingle buildOrder(StructNewOrderSingle objNewOrder) throws Exception {
		try {
			NewOrderSingle newOrder = new NewOrderSingle();
			newOrder.set(new ClOrdID(objNewOrder.getClOrdID()));


			NewOrderSingle.NoPartyIDs grpPartyID = new NewOrderSingle.NoPartyIDs();
			grpPartyID.setField(new PartyID(config.getProperty("PartyID")));	//Get the party Id from Properties file
			grpPartyID.setField(new PartyIDSource('D'));		

			grpPartyID.setField(new PartyRole(13));	
			newOrder.addGroup(grpPartyID);
			//	  	
			newOrder.setField(new NoPartyIDs(1));

			newOrder.setField(new HandlInst(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE)); //1 = Automated execution order, private, no Broker intervention
			newOrder.setField(new Symbol(objNewOrder.getSymbol()));
			newOrder.set(new Side(objNewOrder.getSide()));
			newOrder.set(new TransactTime(new Date()));
			newOrder.set(new OrderQty(objNewOrder.getOrderQty()));
			newOrder.set(new OrdType(OrdType.PREVIOUSLY_QUOTED));
			newOrder.set(new Price(objNewOrder.getPrice()));
			newOrder.setField(new TimeInForce(TimeInForce.FILL_OR_KILL));
			newOrder.set(new QuoteID(objNewOrder.getQuoteID()));
			newOrder.set(new Currency(objNewOrder.getCurrency()));
			return newOrder;
		} catch (Exception e) {
			throw new Exception("Error in buildOrderAndSend() : " + e.getMessage());
		}
	}

}
