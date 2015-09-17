import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.stream.JsonParsingException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;

public class Parser {

	public static final String CANDSET_HEADER = "pairID:INTEGER,A.id:TEXT,B.id:TEXT";
	public static final String GOLD_HEADER = "pairID:INTEGER,A.id:TEXT,B.id:TEXT,label:INTEGER";

	private static Set<String> getAttributeNames(String attrs) throws JsonParsingException {
		Set<String> attributeNames = new HashSet<String>();
		JsonReader reader = Json.createReader(new StringReader(attrs));
		JsonObject obj = reader.readObject();
		attributeNames = obj.keySet();
		return attributeNames;
	}

	private static Set<String> getAttributeNames(String itemJson, String attributeName) throws JsonParsingException {
		Set<String> attributeNames = new HashSet<String>();
		JsonReader reader = Json.createReader(new StringReader(itemJson));
		JsonObject obj = reader.readObject();
		if (obj.containsKey(attributeName)) {
			JsonValue val = obj.get(attributeName);
			JsonObject obj1 = (JsonObject) val;
			attributeNames = obj1.keySet();	
		}
		return attributeNames;
	}
	
	private static String getHeader(Set<String> attributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("id");
		for (String s: attributes) {
			sb.append(",");
			//replace space by underscore
			sb.append(s.replaceAll(" ", "_"));
		}
		return sb.toString();
	}

	private static Map<String, String> parseJsonBlob(String jsonBlob, Set<String> keys) {
		Map<String, String> attribValuePairs = new HashMap<String, String>();
		JsonReader reader = Json.createReader(new StringReader(jsonBlob));
		JsonObject obj = reader.readObject();
		for (String s: keys) {
			if (obj.containsKey(s)) {
				attribValuePairs.put(s, obj.get(s).toString());
			}
			else {
				attribValuePairs.put(s, "");
			}
		}
		return attribValuePairs;
	}

	private static void printCsvRecord(String id1, String attr1, Set<String> attributes, CSVPrinter printer) {
		try {
			printer.print(id1);
			Map<String, String> attribValuePairs = parseJsonBlob(attr1, attributes);
			for (String s: attributes) {
				String value = attribValuePairs.get(s);
				printer.print(value);
			}
			printer.println();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void parseLabeledItemPairs() {
		String dataFilePath = "/Users/sdas7/Downloads/sample_electronics.csv";
		String tableAPath = "A.csv";
		String tableBPath = "B.csv";
		String candsetPath = "candset.csv";
		String goldPath = "gold.csv";

		FileReader r;
		try {
			r = new FileReader(dataFilePath);
			CSVParser parser;
			parser = new CSVParser(r);
			List<CSVRecord> records = parser.getRecords();
			r.close();
			int size = records.size();
			System.out.println("No. of records: " + size);

			Map<String, String> tableA = new HashMap<String, String>();
			Map<String, String> tableB = new HashMap<String, String>();

			Set<String> attributesA = new LinkedHashSet<String>();
			Set<String> attributesB = new LinkedHashSet<String>();

			BufferedWriter candsetBw = new BufferedWriter(new FileWriter(candsetPath, true));
			CSVPrinter candsetPrinter = new CSVPrinter(candsetBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			candsetPrinter.print(CANDSET_HEADER);
			candsetPrinter.println();

			BufferedWriter goldBw = new BufferedWriter(new FileWriter(goldPath, true));
			CSVPrinter goldPrinter = new CSVPrinter(goldBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			goldPrinter.print(GOLD_HEADER);
			goldPrinter.println();

			int badPairs = 0;
			for (int i = 0; i < size; i++) {
				CSVRecord rec = records.get(i);
				int pairId = i + 1; // ignore the pairId coming from the data 
				String id1 = rec.get(1).trim();
				String attr1 = rec.get(2).trim();
				String id2 = rec.get(3).trim();
				String attr2 = rec.get(4).trim();
				String matchLabel = rec.get(5).trim();
				int label = 0;
				if ("MATCH".equals(matchLabel)) {
					label = 1;
				}
				try {
					Set<String> attrA = getAttributeNames(attr1);
					System.out.println("No. of attributes in A record: " + attrA.size());
					attributesA.addAll(attrA);
					if (tableA.containsKey(id1)) {
						System.out.println("Duplicate A id: " + id1);
					}
					else {
						tableA.put(id1, attr1);	
					}
				}
				catch(JsonParsingException jpe) {
					System.err.println("Bad attr1 in tuple pair #" + pairId + ", " + jpe.getMessage());
					badPairs++;
					continue;
				}
				catch(JsonException je) {
					System.err.println("Bad attr1 in tuple pair #" + pairId + ", " + je.getMessage());
					badPairs++;
					continue;
				}
				try {
					Set<String> attrB = getAttributeNames(attr2);
					System.out.println("No. of attributes in B record: " + attrB.size());
					attributesB.addAll(attrB);
					if (tableB.containsKey(id2)) {
						System.out.println("Duplicate B id: " + id2);
					}
					else {
						tableB.put(id2, attr2);	
					}
				}
				catch(JsonParsingException jpe) {
					System.err.println("Bad attr2 in tuple pair #" + pairId + ", " + jpe.getMessage());
					badPairs++;
					continue;
				}
				catch(JsonException je) {
					System.err.println("Bad attr2 in tuple pair #" + pairId + ", " + je.getMessage());
					badPairs++;
					continue;
				}
				candsetPrinter.print(pairId);
				candsetPrinter.print(id1);
				candsetPrinter.print(id2);
				candsetPrinter.println();

				goldPrinter.print(pairId);
				goldPrinter.print(id1);
				goldPrinter.print(id2);
				goldPrinter.print(label);
				goldPrinter.println();
			}
			candsetPrinter.close();
			candsetBw.close();
			goldPrinter.close();
			goldBw.close();

			System.out.println("No. of A tuples: " + tableA.size());
			System.out.println("No. of B tuples: " + tableB.size());
			System.out.println("Removing Item ID from A attributes ...");
			attributesA.remove("Item ID");
			System.out.println("Removing Item ID from B attributes ...");
			attributesB.remove("Item ID");
			System.out.println("No. of attributes in A :" + attributesA.size());
			System.out.println("No. of attributes in B :" + attributesB.size());
			System.out.println("A attributes: ");
			for (String s: attributesA) {
				System.out.println(s);
			}
			System.out.println();

			System.out.println("B attributes: ");
			for (String s: attributesB) {
				System.out.println(s);
			}
			System.out.println("No. of bad pairs: " + badPairs);

			//get header for the tables
			String tableHeader = getHeader(attributesB);

			BufferedWriter tableABw = new BufferedWriter(new FileWriter(tableAPath, true));
			CSVPrinter tableAPrinter = new CSVPrinter(tableABw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			tableAPrinter.print(tableHeader);
			tableAPrinter.println();

			BufferedWriter tableBBw = new BufferedWriter(new FileWriter(tableBPath, true));
			CSVPrinter tableBPrinter = new CSVPrinter(tableBBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			tableBPrinter.print(tableHeader);
			tableBPrinter.println();

			// print table A records
			for (Map.Entry<String, String> entry: tableA.entrySet()) {
				String id1 = entry.getKey();
				String attr1 = entry.getValue();
				printCsvRecord(id1, attr1, attributesB, tableAPrinter);
			}
			tableAPrinter.close();
			tableABw.close();

			// print table A records
			for (Map.Entry<String, String> entry: tableB.entrySet()) {
				String id2 = entry.getKey();
				String attr2 = entry.getValue();
				printCsvRecord(id2, attr2, attributesB, tableBPrinter);
			}
			tableBPrinter.close();
			tableBBw.close();
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void parseTrainTestItemPairs() {
		String trainFilePath = "/Users/sdas7/Downloads/elec_train_30k.csv";
		String testFilePath = "/Users/sdas7/Downloads/elec_test_30K.csv";
		String tableAPath = "walmart_el_30k.csv";
		String tableBPath = "vendor_el_30k.csv";
		String candsetPath = "wv_candset_el_30k.csv";
		String trainPath = "train_el_30k.csv";
		String testPath = "test_el_30k.csv";

		try {
			CSVParser trainParser = new CSVParser(new FileReader(trainFilePath));
			List<CSVRecord> trainRecords = trainParser.getRecords();
			int trainSize = trainRecords.size();
			System.out.println("No. of train records: " + trainSize);

			CSVParser testParser = new CSVParser(new FileReader(testFilePath));
			List<CSVRecord> testRecords = testParser.getRecords();
			int testSize = testRecords.size();
			System.out.println("No. of test records: " + testSize);

			Map<String, String> tableA = new HashMap<String, String>();
			Map<String, String> tableB = new HashMap<String, String>();

			Set<String> attributesA = new LinkedHashSet<String>();
			Set<String> attributesB = new LinkedHashSet<String>();

			BufferedWriter candsetBw = new BufferedWriter(new FileWriter(candsetPath, true));
			CSVPrinter candsetPrinter = new CSVPrinter(candsetBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			candsetPrinter.print(CANDSET_HEADER);
			candsetPrinter.println();

			BufferedWriter trainBw = new BufferedWriter(new FileWriter(trainPath, true));
			CSVPrinter trainPrinter = new CSVPrinter(trainBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			trainPrinter.print(GOLD_HEADER);
			trainPrinter.println();

			int badTrainPairs = 0;
			for (int i = 0; i < trainSize; i++) {
				CSVRecord rec = trainRecords.get(i);
				int pairId = i + 1; // ignore the pairId coming from the data 
				String id1 = rec.get(1).trim();
				String attr1 = rec.get(2).trim();
				String id2 = rec.get(3).trim();
				String attr2 = rec.get(4).trim();
				String matchLabel = rec.get(5).trim();
				int label = 0;
				if ("MATCH".equals(matchLabel)) {
					label = 1;
				}
				try {

					Set<String> attrA = getAttributeNames(attr1);
					//System.out.println("No. of attributes in A record: " + attrA.size());
					attributesA.addAll(attrA);
					if (tableA.containsKey(id1)) {
						//System.out.println("Duplicate A id: " + id1);
					}
					else {
						tableA.put(id1, attr1);	
					}
				}
				catch(JsonParsingException jpe) {
					System.err.println("Bad attr1 in train pair #" + pairId + ", " + jpe.getMessage());
					badTrainPairs++;
					continue;
				}
				catch(JsonException je) {
					System.err.println("Bad attr1 in train pair #" + pairId + ", " + je.getMessage());
					badTrainPairs++;
					continue;
				}
				try {
					Set<String> attrB = getAttributeNames(attr2);
					//System.out.println("No. of attributes in B record: " + attrB.size());
					attributesB.addAll(attrB);
					if (tableB.containsKey(id2)) {
						//System.out.println("Duplicate B id: " + id2);
					}
					else {
						tableB.put(id2, attr2);	
					}
				}
				catch(JsonParsingException jpe) {
					System.err.println("Bad attr2 in train pair #" + pairId + ", " + jpe.getMessage());
					badTrainPairs++;
					continue;
				}
				catch(JsonException je) {
					System.err.println("Bad attr2 in train pair #" + pairId + ", " + je.getMessage());
					badTrainPairs++;
					continue;
				}
				candsetPrinter.print(pairId);
				candsetPrinter.print(id1);
				candsetPrinter.print(id2);
				candsetPrinter.println();

				trainPrinter.print(pairId);
				trainPrinter.print(id1);
				trainPrinter.print(id2);
				trainPrinter.print(label);
				trainPrinter.println();
			}
			trainPrinter.close();
			trainBw.close();

			BufferedWriter testBw = new BufferedWriter(new FileWriter(testPath, true));
			CSVPrinter testPrinter = new CSVPrinter(testBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			testPrinter.print(GOLD_HEADER);
			testPrinter.println();

			int badTestPairs = 0;
			for (int i = 0; i < testSize; i++) {
				CSVRecord rec = testRecords.get(i);
				int pairId = trainSize + i; // ignore the pairId coming from the data 
				String id1 = rec.get(1).trim();
				String attr1 = rec.get(2).trim();
				String id2 = rec.get(3).trim();
				String attr2 = rec.get(4).trim();
				String matchLabel = rec.get(5).trim();
				int label = 0;
				if ("MATCH".equals(matchLabel)) {
					label = 1;
				}
				try {
					Set<String> attrA = getAttributeNames(attr1);
					//System.out.println("No. of attributes in A record: " + attrA.size());
					attributesA.addAll(attrA);
					if (tableA.containsKey(id1)) {
						//System.out.println("Duplicate A id: " + id1);
					}
					else {
						tableA.put(id1, attr1);	
					}
				}
				catch(JsonParsingException jpe) {
					System.err.println("Bad attr1 in test pair #" + pairId + ", " + jpe.getMessage());
					badTestPairs++;
					continue;
				}
				catch(JsonException je) {
					System.err.println("Bad attr1 in test pair #" + pairId + ", " + je.getMessage());
					badTestPairs++;
					continue;
				}
				try {
					Set<String> attrB = getAttributeNames(attr2);
					//System.out.println("No. of attributes in B record: " + attrB.size());
					attributesB.addAll(attrB);
					if (tableB.containsKey(id2)) {
						//System.out.println("Duplicate B id: " + id2);
					}
					else {
						tableB.put(id2, attr2);	
					}
				}
				catch(JsonParsingException jpe) {
					System.err.println("Bad attr2 in test pair #" + pairId + ", " + jpe.getMessage());
					badTestPairs++;
					continue;
				}
				catch(JsonException je) {
					System.err.println("Bad attr2 in test pair #" + pairId + ", " + je.getMessage());
					badTestPairs++;
					continue;
				}
				candsetPrinter.print(pairId);
				candsetPrinter.print(id1);
				candsetPrinter.print(id2);
				candsetPrinter.println();

				testPrinter.print(pairId);
				testPrinter.print(id1);
				testPrinter.print(id2);
				testPrinter.print(label);
				testPrinter.println();
			}
			testPrinter.close();
			testBw.close();

			candsetPrinter.close();
			candsetBw.close();

			System.out.println("No. of A tuples: " + tableA.size());
			System.out.println("No. of B tuples: " + tableB.size());
			System.out.println("No. of attributes in A :" + attributesA.size());
			System.out.println("No. of attributes in B :" + attributesB.size());
			System.out.println("A attributes: ");
			for (String s: attributesA) {
				System.out.println(s);
			}
			System.out.println();

			System.out.println("B attributes: ");
			for (String s: attributesB) {
				System.out.println(s);
			}
			System.out.println("No. of bad train pairs: " + badTrainPairs);
			System.out.println("No. of bad test pairs: " + badTestPairs);
			System.out.println("Removing Item ID from B attributes ...");
			attributesB.remove("Item ID");

			//get header for the tables
			String tableHeader = getHeader(attributesB);

			BufferedWriter tableABw = new BufferedWriter(new FileWriter(tableAPath, true));
			CSVPrinter tableAPrinter = new CSVPrinter(tableABw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			tableAPrinter.print(tableHeader);
			tableAPrinter.println();

			BufferedWriter tableBBw = new BufferedWriter(new FileWriter(tableBPath, true));
			CSVPrinter tableBPrinter = new CSVPrinter(tableBBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			tableBPrinter.print(tableHeader);
			tableBPrinter.println();

			// print table A records
			for (Map.Entry<String, String> entry: tableA.entrySet()) {
				String id1 = entry.getKey();
				String attr1 = entry.getValue();
				printCsvRecord(id1, attr1, attributesB, tableAPrinter);
			}
			tableAPrinter.close();
			tableABw.close();

			// print table B records
			for (Map.Entry<String, String> entry: tableB.entrySet()) {
				String id2 = entry.getKey();
				String attr2 = entry.getValue();
				printCsvRecord(id2, attr2, attributesB, tableBPrinter);
			}
			tableBPrinter.close();
			tableBBw.close();
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void sampleExamplePairs (int numPositives, int numNegatives,
			List<String> attributesToKeep) throws IOException {
		String candsetFilePath = "/Users/sdas7/Downloads/elec_test_30k.csv";
		String samplePairsFilePath = "sample_negative_pairs.txt";
		CSVParser candsetParser = new CSVParser(new FileReader(candsetFilePath));
		List<CSVRecord> candsetRecords = candsetParser.getRecords();
		int candsetSize = candsetRecords.size();
		System.out.println("No. of candset records: " + candsetSize);

		BufferedWriter samplePairsBw = new BufferedWriter(new FileWriter(samplePairsFilePath));

		Set<String> attributes = new LinkedHashSet<String>(attributesToKeep);
		for (int pairId = 0; pairId < numPositives; pairId++) {
			samplePairsBw.write("Positive Item Pair #" + (pairId + 1));
			samplePairsBw.newLine();
			samplePairsBw.write("-------------------------");
			samplePairsBw.newLine();
			CSVRecord record = candsetRecords.get(pairId);
			String id1 = record.get(1).trim();
			String attr1 = record.get(2).trim();
			String id2 = record.get(3).trim();
			String attr2 = record.get(4).trim();
			try {
				Map<String, String> attributeValuePairsA = parseJsonBlob(attr1, attributes);
				Map<String, String> attributeValuePairsB = parseJsonBlob(attr2, attributes);
				samplePairsBw.write("Walmart item (id: " + id1 + ")");
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				
				for (String s: attributes) {
					String val = attributeValuePairsA.get(s);
					if (null == val || val.isEmpty()) {
						continue;
					}
					samplePairsBw.write(s + ": ");
					samplePairsBw.write(val);
					samplePairsBw.newLine();
					samplePairsBw.newLine();
				}
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				samplePairsBw.write("Vendor item (id: " + id2 + ")");
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				
				for (String s: attributes) {
					String val = attributeValuePairsB.get(s);
					if (null == val || val.isEmpty()) {
						continue;
					}
					samplePairsBw.write(s + ": ");
					samplePairsBw.write(val);
					samplePairsBw.newLine();
					samplePairsBw.newLine();
				}
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				String matchLabel = record.get(5).trim();
				System.out.println(matchLabel);
				samplePairsBw.write(matchLabel);
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				samplePairsBw.newLine();
			}
			catch(JsonParsingException jpe) {
				System.err.println("Bad pair #" + pairId + ", " + jpe.getMessage());
				continue;
			}
			catch(JsonException je) {
				System.err.println("Bad pair #" + pairId + ", " + je.getMessage());
				continue;
			}
		}

		for (int pairId = 20000; pairId < 20000 + numNegatives; pairId++) {
			samplePairsBw.write("Negative Item Pair #" + (pairId - 19999));
			samplePairsBw.newLine();
			samplePairsBw.write("-------------------------");
			samplePairsBw.newLine();
			CSVRecord record = candsetRecords.get(pairId);
			String id1 = record.get(1).trim();
			String attr1 = record.get(2).trim();
			String id2 = record.get(3).trim();
			String attr2 = record.get(4).trim();
			try {
				Map<String, String> attributeValuePairsA = parseJsonBlob(attr1, attributes);
				Map<String, String> attributeValuePairsB = parseJsonBlob(attr2, attributes);
				
				samplePairsBw.write("Walmart item (id: " + id1 + ")");
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				
				for (String s: attributes) {
					String val = attributeValuePairsA.get(s);
					if (null == val || val.isEmpty()) {
						continue;
					}
					samplePairsBw.write(s + ": ");
					samplePairsBw.write(val);
					samplePairsBw.newLine();
					samplePairsBw.newLine();
				}
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				
				samplePairsBw.write("Vendor item (id: " + id2 + ")");
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				
				for (String s: attributes) {
					String val = attributeValuePairsB.get(s);
					if (null == val || val.isEmpty()) {
						continue;
					}
					samplePairsBw.write(s + ": ");
					samplePairsBw.write(val);
					samplePairsBw.newLine();
					samplePairsBw.newLine();
				}
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				String matchLabel = record.get(5).trim();
				System.out.println(matchLabel);
				samplePairsBw.write(matchLabel);
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				samplePairsBw.newLine();
			}
			catch(JsonParsingException jpe) {
				System.err.println("Bad pair #" + pairId + ", " + jpe.getMessage());
				continue;
			}
			catch(JsonException je) {
				System.err.println("Bad pair #" + pairId + ", " + je.getMessage());
				continue;
			}
		}
		samplePairsBw.close();
	}

	private static void parseItems() {
		String dataFilePath = "/Users/sdas7/Documents/wlabs_data/CRAWLER_MATCH_WITH_DOTCOM_ANALYSIS_DATA.txt";
		String tableAPath = "Samsung.csv";
		try {
			BufferedReader br = new BufferedReader(new FileReader(dataFilePath));
			String line;

			Map<String, String> table = new HashMap<String, String>();
			Set<String> attributes = new LinkedHashSet<String>();

			int badRecords = 0;
			int id = 1;
			while((line = br.readLine()) != null) {
				try {
					Set<String> attrs = getAttributeNames(line);
					System.out.println("No. of attributes in record #" + id + ": " + attrs.size());
					attributes.addAll(attrs);
					if (table.containsKey(id)) {
						System.out.println("Duplicate id: " + id);
					}
					else {
						table.put(String.valueOf(id), line);	
					}
				}
				catch(JsonParsingException jpe) {
					System.err.println("Bad record #" + id + ": " + jpe.getMessage());
					badRecords++;
					continue;
				}
				catch(JsonException je) {
					System.err.println("Bad record #" + id + ": " + je.getMessage());
					badRecords++;
					continue;
				}
				id++;
			}
			br.close();
			System.out.println("No. of records: " + table.size());
			System.out.println("Removing Item ID from B attributes ...");
			attributes.remove("Item ID");
			System.out.println("No. of attributes in table: " + attributes.size());
			System.out.println("Table attributes: ");
			for (String s: attributes) {
				System.out.println(s);
			}
			System.out.println();
			System.out.println("No. of bad records: " + badRecords);

			//get header for the tables
			String tableHeader = getHeader(attributes);

			BufferedWriter tableBw = new BufferedWriter(new FileWriter(tableAPath, true));
			CSVPrinter tablePrinter = new CSVPrinter(tableBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			tablePrinter.print(tableHeader);
			tablePrinter.println();

			// print table records
			for (Map.Entry<String, String> entry: table.entrySet()) {
				String id1 = entry.getKey();
				String attr1 = entry.getValue();
				printCsvRecord(id1, attr1, attributes, tablePrinter);
			}
			tablePrinter.close();
			tableBw.close();
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void parseElectronicsItems() {
		String dataFilePath = "/Users/sanjib/Documents/walmart_catalog/elec.txt";
		String attributesPath = "tableAttributes.txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(dataFilePath));
			String line;

			Map<String, Integer> attribsCount = new HashMap<String, Integer>();
			
			int badRecords = 0;
			int id = 1;
			while((line = br.readLine()) != null) {
				String[] vals = line.split("\t");
				String itemJson = vals[0];
				try {
				Set<String> attribNames = getAttributeNames(itemJson, "product_attributes");
				for (String s: attribNames) {
					if (attribsCount.containsKey(s)) {
						int value = attribsCount.get(s);
						attribsCount.put(s, value + 1);
					}
					else {
						attribsCount.put(s, 1);
					}
				}
				}
				catch (JsonException e) {
					badRecords++;
				}
				id++;
				//if (id > 2) break;
			}
			br.close();
			System.out.println();
			System.out.println("No. of bad records: " + badRecords);
			for (String a: attribsCount.keySet()) {
				int v = attribsCount.get(a);
				System.out.println(a + ": " + v);
			}
			System.out.println("Total no. of attributes: " + attribsCount.size());
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void parseRuleEvals() {
		String ruleEvalsFilePath = "ruleEvalsCopy.txt";
		String rulesPath = "ruleEvals.csv";
		try {
			BufferedReader br = new BufferedReader(new FileReader(ruleEvalsFilePath));
			BufferedWriter ruleBw = new BufferedWriter(new FileWriter(rulesPath, true));
			CSVPrinter rulePrinter = new CSVPrinter(ruleBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			String line;
			while ((line = br.readLine()) != null) {
				String[] vals = line.split(",");
				String ruleString = vals[0].trim();
				int tp = Integer.parseInt(vals[1].trim().split("=")[1]);
				int fn = Integer.parseInt(vals[2].trim().split("=")[1]);
				int fp = Integer.parseInt(vals[3].trim().split("=")[1]);
				double precision = (100.0 * tp) / (tp + fp);
				double recall = (100.0 * tp) / (tp + fn);
				double f1 = (2 * precision * recall) / (precision + recall);
				rulePrinter.print(ruleString);
				rulePrinter.print(precision);
				rulePrinter.print(recall);
				rulePrinter.print(f1);
				rulePrinter.println();
			}
			br.close();
			rulePrinter.close();
			ruleBw.close();
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void applyRuleOnCrossProduct() {
		String tableAPath = "Samsung.csv";
		String tableBPath = "Amazon.csv";
		String candsetPath = "pn_candset.csv";
		try {
			CSVParser parserA = new CSVParser(new FileReader(tableAPath));
			List<CSVRecord> recordsA = parserA.getRecords();
			CSVParser parserB = new CSVParser(new FileReader(tableBPath));
			List<CSVRecord> recordsB = parserB.getRecords();
			BufferedWriter candsetBw = new BufferedWriter(new FileWriter(candsetPath, true));
			CSVPrinter candsetPrinter = new CSVPrinter(candsetBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			int pairId = 0;
			AbstractStringMetric metric = new JaccardSimilarity();
			for (CSVRecord a: recordsA) {
				String id1 = a.get(0);
				String productNameA = a.get(2);
				for (CSVRecord b: recordsB) {
					String id2 = b.get(0);
					String productNameB = b.get(3);
					if (metric.getSimilarity(productNameA, productNameB) > 0.2) {
						candsetPrinter.print(pairId);
						candsetPrinter.print(id1);
						candsetPrinter.print(id2);
						candsetPrinter.println();
						pairId++;
					}
				}
			}
			candsetPrinter.close();
			candsetBw.close();
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static Map<String, CSVRecord> getTableFromCsv(String tableFilePath) throws FileNotFoundException, IOException {
		Map<String, CSVRecord> table = new HashMap<String, CSVRecord>();
		CSVParser tableParser = new CSVParser(new FileReader(tableFilePath));
		List<CSVRecord> tableRecords = tableParser.getRecords();
		int tableSize = tableRecords.size();
		System.out.println("No. of table records: " + tableSize);
		for (CSVRecord r : tableRecords) {
			String id = r.get(0).trim();
			table.put(id, r);
		}
		return table;
	}

	private static void printCsvRecord(CSVPrinter p, CSVRecord r, String label) throws IOException {
		int size = r.size();
		for (int i = 0; i < size - 1; i++) {
			String s = r.get(i);
			p.print(s);
		}
		p.print(label);
		p.println();
	}

	private static void cleanupLabeledPairs() {
		String inputLabeledPairsFilePath = "train.csv";
		String outputLabeledPairsFilePath = "trainPnPsdPld.csv";
		String tableAFilePath = "walmart.csv";
		String tableBFilePath = "vendor.csv";
		try {
			Map<String, CSVRecord> tableA = getTableFromCsv(tableAFilePath);
			Map<String, CSVRecord> tableB = getTableFromCsv(tableBFilePath);
			CSVParser inputLabeledPairsParser = new CSVParser(new FileReader(inputLabeledPairsFilePath));
			List<CSVRecord> inputLabeledPairsRecords = inputLabeledPairsParser.getRecords();
			int inputLabeledPairsSize = inputLabeledPairsRecords.size();
			System.out.println("No. of input labeled pairs records: " + inputLabeledPairsSize);
			BufferedWriter outBw = new BufferedWriter(new FileWriter(outputLabeledPairsFilePath, true));
			CSVPrinter outPrinter = new CSVPrinter(outBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			int numPairsCleaned = 0;
			for (CSVRecord r : inputLabeledPairsRecords) {
				String label = r.get(3).trim();
				if ("0".equals(label)) {
					String id1 = r.get(1).trim();
					String id2 = r.get(2).trim();
					CSVRecord r1 = tableA.get(id1);
					CSVRecord r2 = tableB.get(id2);
					String pn1 = r1.get(4);
					String psd1 = r1.get(14);
					String pld1 = r1.get(8);
					String pn2 = r2.get(4);
					String psd2 = r2.get(14);
					String pld2 = r2.get(8);
					pn1 = pn1.replaceAll("[^\\dA-Za-z ]", "");
					pn2 = pn2.replaceAll("[^\\dA-Za-z ]", "");
					psd1 = psd1.replaceAll("[^\\dA-Za-z ]", "");
					psd2 = psd2.replaceAll("[^\\dA-Za-z ]", "");
					pld1 = pld1.replaceAll("[^\\dA-Za-z ]", "");
					pld2 = pld2.replaceAll("[^\\dA-Za-z ]", "");
					boolean pnMatch = true;
					if (null != pn1 && null != pn2 &&
							!pn1.isEmpty() && !pn2.isEmpty() &&
							!"null".equals(pn1) && !"null".equals(pn2)) {
						pnMatch = pn1.equalsIgnoreCase(pn2);
					}

					boolean psdMatch = true;
					if (null != psd1 && null != psd2 &&
							!psd1.isEmpty() && !psd2.isEmpty() &&
							!"null".equals(psd1) && !"null".equals(psd2)) {
						psdMatch = psd1.equalsIgnoreCase(psd2);
					}

					boolean pldMatch = true;
					if (null != pld1 && null != pld2 &&
							!pld1.isEmpty() && !pld2.isEmpty() &&
							!"null".equals(pld1) && !"null".equals(pld2)) {
						pldMatch = pld1.equalsIgnoreCase(pld2);
					}

					if (pnMatch && psdMatch && pldMatch) {
						// must be a match
						System.out.println(id1 + ", " + id2);
						label = "1";
						numPairsCleaned++;
					}
					/*
					if ("15042325".equals(id1) && "12343007#eBags".equals(id2)) {
						System.out.println("pnMatch: " + pnMatch
								+ ", psdMatch: " + psdMatch + ", pldMatch: " + pldMatch);
					}
					*/
				}
				printCsvRecord(outPrinter, r, label);
			}
			outPrinter.close();
			System.out.println("Number of pairs cleaned: " + numPairsCleaned);
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void cleanupLabeledPairs2() {
		String inputLabeledPairsFilePath = "train.csv";
		String outputLabeledPairsFilePath = "trainPnPsdPld2.csv";
		String tableAFilePath = "walmart.csv";
		String tableBFilePath = "vendor.csv";
		try {
			Map<String, CSVRecord> tableA = getTableFromCsv(tableAFilePath);
			Map<String, CSVRecord> tableB = getTableFromCsv(tableBFilePath);
			CSVParser inputLabeledPairsParser = new CSVParser(new FileReader(inputLabeledPairsFilePath));
			List<CSVRecord> inputLabeledPairsRecords = inputLabeledPairsParser.getRecords();
			int inputLabeledPairsSize = inputLabeledPairsRecords.size();
			System.out.println("No. of input labeled pairs records: " + inputLabeledPairsSize);
			BufferedWriter outBw = new BufferedWriter(new FileWriter(outputLabeledPairsFilePath, true));
			CSVPrinter outPrinter = new CSVPrinter(outBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
			int numPairsCleaned = 0;
			for (CSVRecord r : inputLabeledPairsRecords) {
				String label = r.get(3).trim();
				if ("0".equals(label)) {
					String id1 = r.get(1).trim();
					String id2 = r.get(2).trim();
					CSVRecord r1 = tableA.get(id1);
					CSVRecord r2 = tableB.get(id2);
					String pn1 = r1.get(4).trim();
					String psd1 = r1.get(14).trim();
					String pld1 = r1.get(8).trim();
					String pn2 = r2.get(4).trim();
					String psd2 = r2.get(14).trim();
					String pld2 = r2.get(8).trim();
					pn1 = pn1.replaceAll("[^\\dA-Za-z ]", "");
					pn2 = pn2.replaceAll("[^\\dA-Za-z ]", "");
					psd1 = psd1.replaceAll("[^\\dA-Za-z ]", "");
					psd2 = psd2.replaceAll("[^\\dA-Za-z ]", "");
					pld1 = pld1.replaceAll("[^\\dA-Za-z ]", "");
					pld2 = pld2.replaceAll("[^\\dA-Za-z ]", "");
					boolean pnMatch = true;
					if (null != pn1 && null != pn2 &&
							!pn1.isEmpty() && !pn2.isEmpty() &&
							!"null".equals(pn1) && !"null".equals(pn2)) {
						pnMatch = pn1.equalsIgnoreCase(pn2);
					}

					boolean psdMatch = true;
					if (null != psd1 && null != psd2 &&
							!psd1.isEmpty() && !psd2.isEmpty() &&
							!"null".equals(psd1) && !"null".equals(psd2)) {
						psdMatch = psd1.equalsIgnoreCase(psd2);
					}

					boolean pldMatch = true;
					if (null != pld1 && null != pld2 &&
							!pld1.isEmpty() && !pld2.isEmpty() &&
							!"null".equals(pld1) && !"null".equals(pld2)) {
						pldMatch = pld1.equalsIgnoreCase(pld2);
					}

					if (pnMatch && psdMatch && pldMatch) {
						// must be a match
						System.out.println(id1 + ", " + id2);
						numPairsCleaned++;
						continue;
					}
					
					if ("19500507".equals(id1) && "19500425#eBags".equals(id2)) {
						System.out.println("pnMatch: " + pnMatch
								+ ", psdMatch: " + psdMatch + ", pldMatch: " + pldMatch);
					}
					
				}
				printCsvRecord(outPrinter, r, label);
			}
			outPrinter.close();
			System.out.println("Number of pairs cleaned: " + numPairsCleaned);
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void sampleData(int numPositives, int numNegatives,
			List<String> attributesToConcat) {
		String trainFilePath = "trainPnPsdPld2.csv";
		String samplePairsFilePath = "sample_train.txt";
		String tableAFilePath = "walmart.csv";
		String tableBFilePath = "vendor.csv";
		Map<String, CSVRecord> tableA = getTableFromCsv(tableAFilePath);
		Map<String, CSVRecord> tableB = getTableFromCsv(tableBFilePath);
		CSVParser trainParser = new CSVParser(new FileReader(trainFilePath));
		List<CSVRecord> trainRecords = trainParser.getRecords();
		int trainSize = trainRecords.size();
		System.out.println("No. of train records: " + trainSize);

		BufferedWriter samplePairsBw = new BufferedWriter(new FileWriter(samplePairsFilePath));

		Set<String> attributes = new LinkedHashSet<String>(attributesToConcat);
		for (int pairId = 0; pairId < numPositives; pairId++) {
			CSVRecord record = trainRecords.get(pairId);
			String attr1 = record.get(2).trim();
			String attr2 = record.get(4).trim();
			try {
				Map<String, String> attributeValuePairsA = parseJsonBlob(attr1, attributes);
				Map<String, String> attributeValuePairsB = parseJsonBlob(attr2, attributes);
				for (String s: attributes) {
					String val = attributeValuePairsA.get(s);
					samplePairsBw.write(val);
					samplePairsBw.newLine();
					samplePairsBw.newLine();
				}
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				for (String s: attributes) {
					String val = attributeValuePairsB.get(s);
					samplePairsBw.write(val);
					samplePairsBw.newLine();
					samplePairsBw.newLine();
				}
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				String matchLabel = record.get(5).trim();
				System.out.println(matchLabel);
				samplePairsBw.write(matchLabel);
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
			}
			catch(JsonParsingException jpe) {
				System.err.println("Bad pair #" + pairId + ", " + jpe.getMessage());
				continue;
			}
			catch(JsonException je) {
				System.err.println("Bad pair #" + pairId + ", " + je.getMessage());
				continue;
			}
		}

		for (int pairId = 6000; pairId < 6000 + numNegatives; pairId++) {
			CSVRecord record = candsetRecords.get(pairId);
			String attr1 = record.get(2).trim();
			String attr2 = record.get(4).trim();
			try {
				Map<String, String> attributeValuePairsA = parseJsonBlob(attr1, attributes);
				Map<String, String> attributeValuePairsB = parseJsonBlob(attr2, attributes);
				for (String s: attributes) {
					String val = attributeValuePairsA.get(s);
					samplePairsBw.write(val);
					samplePairsBw.newLine();
					samplePairsBw.newLine();
				}
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				for (String s: attributes) {
					String val = attributeValuePairsB.get(s);
					samplePairsBw.write(val);
					samplePairsBw.newLine();
					samplePairsBw.newLine();
				}
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				String matchLabel = record.get(5).trim();
				System.out.println(matchLabel);
				samplePairsBw.write(matchLabel);
				samplePairsBw.newLine();
				samplePairsBw.newLine();
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
				samplePairsBw.write("-----------------------------------------");
				samplePairsBw.newLine();
			}
			catch(JsonParsingException jpe) {
				System.err.println("Bad pair #" + pairId + ", " + jpe.getMessage());
				continue;
			}
			catch(JsonException je) {
				System.err.println("Bad pair #" + pairId + ", " + je.getMessage());
				continue;
			}
		}
		samplePairsBw.close();
	}
	
	public static void main(String[] args) {
		//parseItems();
		//parseLabeledItemPairs();
		//parseTrainTestItemPairs();
		//parseRuleEvals();
		//applyRuleOnCrossProduct();
//		List<String> attributes = new ArrayList<String>();
//		attributes.add("Product Name");
//		attributes.add("Product Short Description");
//		attributes.add("Product Long Description");
//		attributes.add("Product Segment");
//		attributes.add("Product Type");
//		attributes.add("Brand");
//		attributes.add("Manufacturer");
//		attributes.add("Color");
//		attributes.add("Actual Color");
//		attributes.add("Assembled Product Length");
//		attributes.add("Assembled Product Width");
//		attributes.add("Assembled Product Height");
//		attributes.add("UPC");
//		attributes.add("Manufacturer Part Number");
//		try {
//			sampleExamplePairs(0, 50, attributes);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//cleanupLabeledPairs2();
		parseElectronicsItems();
	}
}