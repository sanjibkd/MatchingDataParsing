import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
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

	public static final String[] attributesToignore = {"Item ID", "GTIN", "Product Segment",
		"Warranty Length", "Country of Origin: Components", "Category", "Composite Wood Code", "Warranty Information",
		"Type", "Video Game Platform"};

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

	private static void addIncr(Map<String, Integer> dictionary, String key) {
		int value = 0;
		if (dictionary.containsKey(key)) {
			value = dictionary.get(key);
		}
		dictionary.put(key, value + 1);
	}

	private static void addIncr(Map<String, Integer> dictionary, String key, int increment) {
		int value = 0;
		if (dictionary.containsKey(key)) {
			value = dictionary.get(key);
		}
		dictionary.put(key, value + increment);
	}

	private static void displayMap(Map<String, Integer> map) {
		for (String k: map.keySet()) {
			int v = map.get(k);
			System.out.println(k + ": " + v);
		}
	}

	private static void dumpMaps(String[] outputFileNames, List<Map<String, Integer>> maps) throws FileNotFoundException {
		for (int i = 0; i < outputFileNames.length; i++) {
			System.out.println("Output dictionary file: " + outputFileNames[i]);
			dumpMap(outputFileNames[i], maps.get(i));
		}
	}

	private static void dumpMap(String outputFileName, Map<String, Integer> map) throws FileNotFoundException {
		PrintWriter pw = new PrintWriter(outputFileName);
		for (String k: map.keySet()) {
			int v = map.get(k);
			pw.println(k + "\t" + v);
		}
		pw.close();
		System.out.println("Size of dictionary: " + map.size());
	}

	private static void createDictionaries(String[] inputFileNames, String[] outputFileNames, String[] attributeNames) throws FileNotFoundException {
		List<Map<String, Integer>> dictionaries = new ArrayList<Map<String, Integer>>(attributeNames.length);
		for (int i = 0; i < attributeNames.length; i++) {
			dictionaries.add(new HashMap<String, Integer>());
		}
		try {
			for (int i = 0; i < inputFileNames.length; i++) {
				String inputFileName = inputFileNames[i];
				BufferedReader br = new BufferedReader(new FileReader(inputFileName));
				int badRecords = 0; // invalid JSON
				int badRecords1 = 0; // no "product_attributes"
				int[] badRecords2 = new int[attributeNames.length]; // no attributeName
				for (int k = 0; k < badRecords2.length; k++) {
					badRecords2[k] = 0;
				}
				int badRecords3 = 0; // no "values"
				int id = 0;
				String line;
				while((line = br.readLine()) != null) {
					if (id % 100000 == 0) {
						System.out.println("Processed " + id + " records of file " + inputFileName);
					}
					String[] vals = line.split("\t");
					String itemJson = vals[0];
					try {
						JsonReader reader = Json.createReader(new StringReader(itemJson));
						JsonObject obj = reader.readObject();
						if (obj.containsKey("product_attributes")) {
							//System.out.println("Found product_attributes");
							JsonObject obj1 = obj.getJsonObject("product_attributes");
							if (null == obj1) {
								badRecords1++;
								id++;
								continue;
							}
							for (int j = 0; j < attributeNames.length; j++) {
								String attributeName = attributeNames[j];
								JsonObject obj2 = obj1.getJsonObject(attributeName);
								if (null == obj2) {
									badRecords2[j]++;
									continue;
								}
								JsonArray arr = obj2.getJsonArray("values");
								if (null == arr) {
									badRecords3++;
									continue;
								}
								for (int k = 0; k < arr.size(); k++) {
									JsonObject obj3 = arr.getJsonObject(k);
									if (arr.size() == 1) {
										String value = obj3.getString("value");
										addIncr(dictionaries.get(j), value);
										break;
									}
									if (obj3.containsKey("isPrimary")) {
										//System.out.println("Found isPrimary");
										if ("true".equals(obj3.getString("isPrimary"))) {
											String value = obj3.getString("value");
											addIncr(dictionaries.get(j), value);
											break;
										}
									}
								}
							}
						}
					}
					catch (JsonException e) {
						badRecords++;
					}
					id++;
				}
				br.close();
				System.out.println("Input File: " + inputFileName);
				System.out.println("No. of records seen: " + id);
				System.out.println("No. of records with Invalid JSON: " + badRecords);
				System.out.println("No. of records with missing product attributes: " + badRecords1);
				for (int j = 0; j < attributeNames.length; j++) {
					String attributeName = attributeNames[j];
					System.out.println("No. of records with missing " + attributeName + ": " + badRecords2[j]);
				}
				System.out.println("No. of records with missing values: " + badRecords3);
			}
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dumpMaps(outputFileNames, dictionaries);
	}

	private static void createDictionary(String inputFileName, String outputFileName, String attributeName) throws FileNotFoundException {
		Map<String, Integer> dictionary = new HashMap<String, Integer>(); // attribute value -> count
		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFileName));
			int badRecords = 0; // invalid JSON
			int badRecords1 = 0; // no "product_attributes"
			int badRecords2 = 0; // no "brand"
			int badRecords3 = 0; // no "values"
			int id = 0;
			String line;
			while((line = br.readLine()) != null) {
				if (id % 100000 == 0) {
					System.out.println("Processed " + id + " records");
				}
				String[] vals = line.split("\t");
				String itemJson = vals[0];
				try {
					JsonReader reader = Json.createReader(new StringReader(itemJson));
					JsonObject obj = reader.readObject();
					if (obj.containsKey("product_attributes")) {
						//System.out.println("Found product_attributes");
						JsonObject obj1 = obj.getJsonObject("product_attributes");
						if (null == obj1) {
							badRecords1++;
							id++;
							continue;
						}
						JsonObject obj2 = obj1.getJsonObject(attributeName);
						if (null == obj2) {
							badRecords2++;
							id++;
							continue;
						}
						JsonArray arr = obj2.getJsonArray("values");
						if (null == arr) {
							badRecords3++;
							id++;
							continue;
						}
						for (int i = 0; i < arr.size(); i++) {
							JsonObject obj3 = arr.getJsonObject(i);
							if (obj3.containsKey("isPrimary")) {
								//System.out.println("Found isPrimary");
								if ("true".equals(obj3.getString("isPrimary"))) {
									String value = obj3.getString("value");
									addIncr(dictionary, value);
									break;
								}
							}
						}
					}
				}
				catch (JsonException e) {
					badRecords++;
				}
				id++;
			}
			br.close();
			System.out.println("No. of records seen: " + id);
			System.out.println("No. of records with Invalid JSON: " + badRecords);
			System.out.println("No. of records with missing product attributes: " + badRecords1);
			System.out.println("No. of records with missing " + attributeName + ": " + badRecords2);
			System.out.println("No. of records with missing values: " + badRecords3);
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dumpMap(outputFileName,dictionary);
	}

	/*
	private static Map<String, Integer> sortByValue (Map<String, Integer> map) {
		ValueComparator vc =  new ValueComparator(map);
		Map<String,Integer> sortedMap = new TreeMap<String,Integer>(vc);
		sortedMap.putAll(map);
		return sortedMap;
	}
	 */
	
	private static boolean extract(JsonObject obj, String[] attributesToExclude) {
		for (String s: attributesToExclude) {
			if (obj.containsKey(s)) {
				return false;
			}
		}
		return true;
	}

	private static void parseElectronicsItemPair(String outputPath, String[] attributesToExclude, String attributeToExtract) {
		String dataFilePath = "/Users/patron/sanjib_electronics_train.txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(dataFilePath));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath, true));
			Map<String, Object> properties = new HashMap<String, Object>(1);
			properties.put(JsonGenerator.PRETTY_PRINTING, true);
			JsonWriterFactory writerFactory = Json.createWriterFactory(properties);
			JsonWriter jsonWriter;

			int badRecords = 0;
			int pairsSeen = 0;
			int id = 1;
			String line;
			while((line = br.readLine()) != null) {
				String[] vals = line.split("\\?");
				String pairId = vals[0];
				String id1 = vals[1];
				String item1json = vals[2];
				//System.out.println(item1json);
				JsonReader reader1 = Json.createReader(new StringReader(item1json));
				try {
					JsonObject obj1 = reader1.readObject();
					if (extract(obj1, attributesToExclude)) {
						Set<String> keySet1 = new HashSet<String>(obj1.keySet());
						for (String s: attributesToignore) {
							keySet1.remove(s);
						}
						for (String s: attributesToExclude) {
							keySet1.remove(s);
						}
						//System.out.println("Obj1 Keyset: " + obj1.keySet());
						//		    		for (String key: attributesToignore) {
						//		    			obj1.remove(key);
						//		    		}
						//System.out.println("No. of keys: " + obj.keySet());
						bw.write(id + ". Walmart product (item id:" + id1 + ")");
						bw.newLine();
						bw.write("****************************************");
						bw.newLine();
						for (String key: keySet1) {
							JsonValue value = obj1.get(key);
							bw.write(key + ": " + value);
							bw.newLine();
							bw.newLine();
						}
						bw.write(attributeToExtract + ": ");
						bw.newLine();
						bw.write("*******************************************************");
						bw.newLine();
						bw.write("*******************************************************");
						bw.newLine();
						bw.newLine();
						id++;		
					}
				}
				catch (JsonParsingException e) {
					badRecords++;
				}
				catch (JsonException e) {
					badRecords++;
				}
				String id2 = vals[3];
				String item2json = vals[4];
				JsonReader reader2 = Json.createReader(new StringReader(item2json));
				try {
					JsonObject obj2 = reader2.readObject();
					if (extract(obj2, attributesToExclude)) {
						Set<String> keySet2 = new HashSet<String>(obj2.keySet());
						for (String s: attributesToignore) {
							keySet2.remove(s);
						}
						for (String s: attributesToExclude) {
							keySet2.remove(s);
						}
						//	    		System.out.println("obj2 keyset: " + obj2.keySet());
						//	    		System.out.println("obj2 contains Item ID: " + obj2.containsKey("Item ID"));
						//	    		System.out.println(obj2.get("Item ID"));
						//	    		for (String key: attributesToignore) {
						//	    			obj2.remove(key);
						//	    		}
						//System.out.println(obj2.keySet());
						bw.write(id + ". Vendor product (item id:" + id2 + ")");
						bw.newLine();
						bw.write("***************************************");
						bw.newLine();
						for (String key: keySet2) {
							JsonValue value = obj2.get(key);
							bw.write(key + ": " + value);
							bw.newLine();
							bw.newLine();
						}
						bw.write(attributeToExtract + ": ");
						bw.newLine();
						bw.write("*******************************************************");
						bw.newLine();
						bw.write("*******************************************************");
						bw.newLine();
						bw.newLine();
						String label = vals[5];
						//				System.out.println("pairId: " + pairId);
						//				System.out.println("id1: " + id1);
						//				System.out.println("id2: " + id2);
						//				System.out.println("label: " + label);
						id++;
					}
				}
				catch(JsonParsingException e) {
					badRecords++;
				}
				catch (JsonException e) {
					badRecords++;
				}
				pairsSeen++;
				if (id > 600) break;
			}
			//jsonWriter.close();
			br.close();
			bw.close();
			System.out.println();
			System.out.println("No. of bad records: " + badRecords);
			System.out.println("No. of pairs seen " + pairsSeen);
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

	private static Set<String> doNotExtract(JsonObject obj, String[] attributesToExclude) {
		Set<String> attributesPresent = new HashSet<String>();
		for (String s: attributesToExclude) {
			if (obj.containsKey(s)) {
				attributesPresent.add(s);
			}
		}
		return attributesPresent;
	}

	private static void prepareSampleForStudents(String outputPath, String[] attributesToExclude, String[] attributesToExtract) {
		String dataFilePath = "/u/s/a/sanjibkd/Downloads/sanjib_electronics_train_325.txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(dataFilePath));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath, true));
			Map<String, Object> properties = new HashMap<String, Object>(1);
			properties.put(JsonGenerator.PRETTY_PRINTING, true);
			JsonWriterFactory writerFactory = Json.createWriterFactory(properties);
			JsonWriter jsonWriter;

			int badRecords = 0;
			int pairsSeen = 0;
			int id = 1;
			String line;
			while((line = br.readLine()) != null) {
				String[] vals = line.split("\\?");
				String pairId = vals[0];
				String id1 = vals[1];
				String item1json = vals[2];
				//System.out.println(item1json);
				JsonReader reader1 = Json.createReader(new StringReader(item1json));
				try {
					JsonObject obj1 = reader1.readObject();
					//Set<String> attributesPresent = doNotExtract(obj1, attributesToExtract);
					//if (attributesPresent.size() != attributesToExtract.length) {
					Set<String> keySet1 = new HashSet<String>(obj1.keySet());
					for (String s: attributesToignore) {
						keySet1.remove(s);
					}
					//						for (String s: attributesToExclude) {
					//							keySet1.remove(s);
					//						}
					//						for (String s: attributesPresent) {
					//							keySet1.add(s);
					//						}
					//System.out.println("Obj1 Keyset: " + obj1.keySet());
					//		    		for (String key: attributesToignore) {
					//		    			obj1.remove(key);
					//		    		}
					//System.out.println("No. of keys: " + obj.keySet());
					bw.write(id + ". Walmart product (item id:" + id1 + ")");
					bw.newLine();
					//bw.write("****************************************");
					//bw.newLine();
					for (String key: keySet1) {
						JsonValue value = obj1.get(key);
						bw.write(key + ": " + value);
						bw.newLine();
						bw.newLine();
					}
					for (String s: attributesToExtract) {
						//							if (attributesPresent.contains(s)) {
						//								continue;
						//							}
						bw.write(s.toUpperCase() + ": ");
						bw.newLine();
					}
					bw.write("*********************************************************************************************************");
					bw.newLine();
					//						bw.write("*******************************************************");
					//						bw.newLine();
					bw.newLine();
					id++;		
					//}
				}
				catch (JsonParsingException e) {
					badRecords++;
				}
				catch (JsonException e) {
					badRecords++;
				}
				String id2 = vals[3];
				String item2json = vals[4];
				JsonReader reader2 = Json.createReader(new StringReader(item2json));
				try {
					JsonObject obj2 = reader2.readObject();
					//Set<String> attributesPresent = doNotExtract(obj2, attributesToExtract);
					//if (attributesPresent.size() != attributesToExtract.length) {
					Set<String> keySet2 = new HashSet<String>(obj2.keySet());
					for (String s: attributesToignore) {
						keySet2.remove(s);
					}
					//						for (String s: attributesToExclude) {
					//							keySet2.remove(s);
					//						}
					//						for (String s: attributesPresent) {
					//							keySet2.add(s);
					//						}
					//	    		System.out.println("obj2 keyset: " + obj2.keySet());
					//	    		System.out.println("obj2 contains Item ID: " + obj2.containsKey("Item ID"));
					//	    		System.out.println(obj2.get("Item ID"));
					//	    		for (String key: attributesToignore) {
					//	    			obj2.remove(key);
					//	    		}
					//System.out.println(obj2.keySet());
					bw.write(id + ". Vendor product (item id:" + id2 + ")");
					bw.newLine();
					//bw.write("***************************************");
					//bw.newLine();
					for (String key: keySet2) {
						JsonValue value = obj2.get(key);
						bw.write(key + ": " + value);
						bw.newLine();
						bw.newLine();
					}
					for (String s: attributesToExtract) {
						//							if (attributesPresent.contains(s)) {
						//								continue;
						//							}
						bw.write(s.toUpperCase() + ": ");
						bw.newLine();
					}
					bw.write("*********************************************************************************************************");
					bw.newLine();
					//						bw.write("*******************************************************");
					//						bw.newLine();
					bw.newLine();
					String label = vals[5];
					//				System.out.println("pairId: " + pairId);
					//				System.out.println("id1: " + id1);
					//				System.out.println("id2: " + id2);
					//				System.out.println("label: " + label);
					id++;
					//}
				}
				catch(JsonParsingException e) {
					badRecords++;
				}
				catch (JsonException e) {
					badRecords++;
				}
				pairsSeen++;
				//if (id > 600) break;
			}
			//jsonWriter.close();
			br.close();
			bw.close();
			System.out.println();
			System.out.println("No. of bad records: " + badRecords);
			System.out.println("No. of pairs seen " + pairsSeen);
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

	private static void printTuple(BufferedWriter bw, JsonObject obj) throws IOException {
		Set<String> keySet = new HashSet<String>(obj.keySet());
		for (String s: attributesToignore) {
			keySet.remove(s);
		}
		for (String key: keySet) {
			JsonValue value = obj.get(key);
			bw.write(key + ": " + value);
			bw.newLine();
			bw.newLine();
		}
	}

	/*
	private static void prepareTuplePairsForStudents(String outputPath) {
		String dataFilePath = "/u/s/a/sanjibkd/Downloads/sanjib_electronics_train_325.txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(dataFilePath));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath, true));
			Map<String, Object> properties = new HashMap<String, Object>(1);
			properties.put(JsonGenerator.PRETTY_PRINTING, true);
			JsonWriterFactory writerFactory = Json.createWriterFactory(properties);
			JsonWriter jsonWriter;

			int badRecords = 0;
			int pairsSeen = 0;
			int id = 1;
			String line;
			while((line = br.readLine()) != null) {
				String[] vals = line.split("\\?");
				String pairId = vals[0];
				String id1 = vals[1];
				String item1json = vals[2];
				//System.out.println(item1json);
				JsonReader reader1 = Json.createReader(new StringReader(item1json));
				String id2 = vals[3];
				String item2json = vals[4];
				JsonReader reader2 = Json.createReader(new StringReader(item2json));
				try {
					JsonObject obj1 = reader1.readObject();
					JsonObject obj2 = reader2.readObject();

					bw.write("Product pair #" + (pairsSeen + 1));
					bw.newLine();
					bw.write("Walmart product (item id:" + id1 + ")");
					bw.newLine();
					printTuple(bw, obj1);
					bw.newLine();
					bw.write("Walmart product (item id:" + id1 + ")");
					bw.newLine();
					printTuple(bw, obj1);
					bw.newLine();

					bw.write("*********************************************************************************************************");
					bw.newLine();
//						bw.write("*******************************************************");
//						bw.newLine();
						bw.newLine();
						id++;		
					//}
				}
				catch (JsonParsingException e) {
					badRecords++;
				}
				catch (JsonException e) {
					badRecords++;
				}
				try {

					//Set<String> attributesPresent = doNotExtract(obj2, attributesToExtract);
					//if (attributesPresent.size() != attributesToExtract.length) {
						Set<String> keySet2 = new HashSet<String>(obj2.keySet());
						for (String s: attributesToignore) {
							keySet2.remove(s);
						}
//						for (String s: attributesToExclude) {
//							keySet2.remove(s);
//						}
//						for (String s: attributesPresent) {
//							keySet2.add(s);
//						}
						//	    		System.out.println("obj2 keyset: " + obj2.keySet());
						//	    		System.out.println("obj2 contains Item ID: " + obj2.containsKey("Item ID"));
						//	    		System.out.println(obj2.get("Item ID"));
						//	    		for (String key: attributesToignore) {
						//	    			obj2.remove(key);
						//	    		}
						//System.out.println(obj2.keySet());
						bw.write(id + ". Vendor product (item id:" + id2 + ")");
						bw.newLine();
						//bw.write("***************************************");
						//bw.newLine();
						for (String key: keySet2) {
							JsonValue value = obj.get(key);
							bw.write(key + ": " + value);
							bw.newLine();
							bw.newLine();
						}
						for (String s: attributesToExtract) {
//							if (attributesPresent.contains(s)) {
//								continue;
//							}
							bw.write(s.toUpperCase() + ": ");
							bw.newLine();
						}
						bw.write("*********************************************************************************************************");
						bw.newLine();
//						bw.write("*******************************************************");
//						bw.newLine();
						bw.newLine();
						String label = vals[5];
						//				System.out.println("pairId: " + pairId);
						//				System.out.println("id1: " + id1);
						//				System.out.println("id2: " + id2);
						//				System.out.println("label: " + label);
						id++;
					//}
				}
				catch(JsonParsingException e) {
					badRecords++;
				}
				catch (JsonException e) {
					badRecords++;
				}
				pairsSeen++;
				//if (id > 600) break;
			}
			//jsonWriter.close();
			br.close();
			bw.close();
			System.out.println();
			System.out.println("No. of bad records: " + badRecords);
			System.out.println("No. of pairs seen " + pairsSeen);
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
	 */

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
	/*
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
	 */

	private static void mergeDictionaries(String[] inputFileNames, String outputFileName) throws IOException {
		Map<String, Integer> dictionary = new HashMap<String, Integer>();
		for (int i = 0; i < inputFileNames.length; i++) {
			String inputFileName = inputFileNames[i];
			BufferedReader br = new BufferedReader(new FileReader(inputFileName));
			int badRecords = 0;
			String line;
			while ((line = br.readLine()) != null) {
				String[] vals = line.split("\\t");
				if (vals.length != 2) {
					badRecords++;
					continue;
				}
				String key = vals[0].trim();
				int value = Integer.parseInt(vals[1].trim());
				addIncr(dictionary, key, value);
			}
			System.out.println("No. of bad records in file " + inputFileName + ": " + badRecords);
			br.close();
		}
		dumpMap(outputFileName, dictionary);
	}

	private static void runMergeDictionaries() {
		String[] inputFileNames = {"/u/s/a/sanjibkd/Downloads/art_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/baby_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/cm_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bi_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/cpo_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/ee_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/null_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/gcgc_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/re_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/fb_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/sw_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/tickets_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/tg_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/tla_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/ps_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/hb_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/crafts_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/so_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/vpa_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/mipa_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/os_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/th_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/jgw_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/default_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/csa_upc_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_upc_dictionary.txt",
		"/u/s/a/sanjibkd/Downloads/hg_upc_dictionary.txt"};
		String outputFileName = "/u/s/a/sanjibkd/Downloads/all_upc_dictionary.txt";
		try {
			mergeDictionaries(inputFileNames, outputFileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void getItems(String inputFileName, String outputFileName, Set<String> itemIds) throws FileNotFoundException {
		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFileName));
			PrintWriter pw = new PrintWriter(outputFileName);
			int badRecords = 0; // invalid JSON
			int badRecords1 = 0; // no "product_attributes"
			int badRecords2 = 0; // no "item_id"
			int badRecords3 = 0; // no "values"
			int id = 0;
			String line;
			while((line = br.readLine()) != null) {
				if (id % 100000 == 0) {
					System.out.println("Processed " + id + " records");
				}
				String[] vals = line.split("\t");
				String itemJson = vals[0];
				try {
					JsonReader reader = Json.createReader(new StringReader(itemJson));
					JsonObject obj = reader.readObject();
					if (obj.containsKey("product_attributes")) {
						//System.out.println("Found product_attributes");
						JsonObject obj1 = obj.getJsonObject("product_attributes");
						if (null == obj1) {
							badRecords1++;
							id++;
							continue;
						}
						JsonObject obj2 = obj1.getJsonObject("item_id");
						if (null == obj2) {
							badRecords2++;
							id++;
							continue;
						}
						JsonArray arr = obj2.getJsonArray("values");
						if (null == arr) {
							badRecords3++;
							id++;
							continue;
						}
						for (int i = 0; i < arr.size(); i++) {
							JsonObject obj3 = arr.getJsonObject(i);
							if (arr.size() == 1) {
								String value = obj3.getString("value");
								if (itemIds.contains(value)) {
									System.out.println("Found item_id " + value);
									pw.println(itemJson);
								}
								break;
							}
							if (obj3.containsKey("isPrimary")) {
								//System.out.println("Found isPrimary");
								if ("true".equals(obj3.getString("isPrimary"))) {
									String value = obj3.getString("value");
									if (itemIds.contains(value)) {
										System.out.println("Found item_id " + value);
										pw.println(itemJson);
									}
									break;
								}
							}
						}
					}
				}
				catch (JsonException e) {
					badRecords++;
				}
				id++;
			}
			br.close();
			pw.close();
			System.out.println("No. of records seen: " + id);
			System.out.println("No. of records with Invalid JSON: " + badRecords);
			System.out.println("No. of records with missing product attributes: " + badRecords1);
			System.out.println("No. of records with missing item_id: " + badRecords2);
			System.out.println("No. of records with missing values: " + badRecords3);
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

	private static void runGetItems() {
		String inputFileName = "/media/My Book/sanjib/walmart_catalog/electronics/elec.txt";
		String outputFileName = "/u/s/a/sanjibkd/Downloads/enriched_walmart_items.txt";
		String[] itemIds = {"4217002", "5347115", "8245516", "8586825", "9193948", "3377768",
				"4217014", "9447294", "3356833", "3377763", "3377767", "3356835",
				"8586819", "9225287", "9225291", "8586826", "9225276", "4805973",
				"9225280", "9225292", "4216995", "9875796", "9225273", "9225293",
				"3356836", "3371668", "4217000", "8586822", "9225274", "4217003",
				"9225275", "3985006", "3356838", "5996481", "5996470", "9863777",
				"9225288", "3356840", "9208106", "9225277", "9722003", "10073803",
				"10910315", "11068335", "11082496", "11084238", "11988059", "11988076",
				"11997515", "12180028", "12180070", "12180103", "12181608", "12181609",
				"12181620", "12181700", "12181705", "12182239", "12182374", "12182400",
				"12182846", "12183204", "932031", "1234894", "3756896", "4025874",
				"7754443", "9189660", "9225261", "9871188", "12184525", "12184565",
				"872051", "2585639", "3387612", "3576485", "12186203", "12186211",
				"12186335", "4664464", "5723587", "12187451", "12187705", "12187816",
				"12187871", "12342530", "12343007", "12343021", "9225253", "2445355",
				"2585638", "2685981", "17771102", "12360588", "12361423", "7964947"};
		Set<String> itemIdsToLookup = new HashSet<String>();
		for (String s: itemIds) {
			itemIdsToLookup.add(s);
		}
		try {
			getItems(inputFileName, outputFileName, itemIdsToLookup);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void writeTable(String fileName, Map<String, String> table, String header, Set<String> attributes) throws IOException {
		BufferedWriter tableBw = new BufferedWriter(new FileWriter(fileName, true));
		CSVPrinter tablePrinter = new CSVPrinter(tableBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
		tablePrinter.print(header);
		tablePrinter.println();
		for (Map.Entry<String, String> entry: table.entrySet()) {
			String id = entry.getKey();
			String rec = entry.getValue();
			try {
				printCsvRecord(id, rec, attributes, tablePrinter);
			}
			catch (JsonParsingException e) {
				System.out.println("id: " + id);
				e.printStackTrace();
			}
		}
		tablePrinter.close();
		tableBw.close();
	}

	private static void createTablesFromLabeledPairs(String labeledPairsFile,
			String table1FileName, String table2FileName, String goldFile, String[] attributeNames) throws IOException {

		Map<String, String> tableA = new HashMap<String, String>();
		Map<String, String> tableB = new HashMap<String, String>();

		BufferedReader br = new BufferedReader(new FileReader(labeledPairsFile));
		BufferedWriter goldBw = new BufferedWriter(new FileWriter(goldFile, true));
		CSVPrinter goldPrinter = new CSVPrinter(goldBw, CSVFormat.DEFAULT.toBuilder().withRecordSeparator("\n").build());
		goldPrinter.print(GOLD_HEADER);
		goldPrinter.println();
		int badRecords = 0;
		int pairsSeen = 0;
		int pairId = 1;
		String line;
		while((line = br.readLine()) != null) {
			String[] vals = line.split("\\?");
			String id1 = vals[1];
			String item1json = vals[2];
			String id2 = vals[3];
			String item2json = vals[4];
			String label = vals[5];
			if (!tableA.containsKey(id1)) {
				tableA.put(id1, item1json);
			}
			if (!tableB.containsKey(id2)) {
				tableB.put(id2, item2json);
			}
			goldPrinter.print(pairId);
			goldPrinter.print(id1);
			goldPrinter.print(id2);
			if ("MATCH".equals(label)) {
				goldPrinter.print(1);
			}
			else {
				goldPrinter.print(0);
			}
			goldPrinter.println();
			pairId++;
		}
		br.close();
		goldPrinter.close();
		goldBw.close();
		
		Set<String> attributes = new LinkedHashSet<String>();
		for (String s: attributeNames) {
			attributes.add(s);
		}
		//get header for the tables
		String tableHeader = getHeader(attributes);
		writeTable(table1FileName, tableA, tableHeader, attributes);
		writeTable(table2FileName, tableB, tableHeader, attributes);
	}

	private static void runCreateTablesFromLabeledPairs() {
		String labeledPairsFile = "/Users/sanjib/Downloads/sample_elec_pairs.txt";
		String table1FileName = "/Users/sanjib/Downloads/walmart.csv";
		String table2FileName = "/Users/sanjib/Downloads/vendor.csv";
		String goldFileName = "/Users/sanjib/Downloads/labeled_325.csv";
		String[] attributeNames = {"Product Name", "Product Short Description", "Product Long Description",
									"Product Type", "Brand", "Manufacturer", "Model", "Color", "Actual Color",
									"Package Quantity", "Assembled Product Length", "Assembled Product Width",
									"Assembled Product Height", "Assembled Product Weight", "Size", "Material",
									"Screen Size", "Laptop Compartment Dimensions", "Print Color", "Page Yield",
									"Manufacturer Part Number", "UPC"};
		try {
			createTablesFromLabeledPairs(labeledPairsFile, table1FileName, table2FileName, goldFileName, attributeNames);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private static void collateExtractedFile(String inputFileName, String outputFileName, String[] attributeNames, String tableName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(inputFileName));
		Set<String> attributes = new HashSet<String>();
		Map<String, Map<String, String>> table = new LinkedHashMap<String, Map<String, String>>();
		for (String s: attributeNames) {
			attributes.add(s);
		}
		String line;
		while ((line = br.readLine()) != null) {
			if (line.contains(tableName + " product")) {
				String[] vals = line.split(":");
				String itemId = vals[1].substring(0, vals[1].length()-1);
				Map<String, String> itemMap = new HashMap<String, String>();
				String newLine;
				while (!(newLine = br.readLine()).startsWith("***")) {
					String[] newVals = newLine.split(": ");
					if (newVals.length >= 2) {
						if (attributes.contains(newVals[0])) {
							String v = newVals[1].trim();
							itemMap.put(newVals[0], v);
						}
					}
				}
				table.put(itemId, itemMap);
			}
		}
		br.close();
		System.out.println("Size of table: " + table.size());
		for (String itemId: table.keySet()) {
			System.out.print(itemId + ": ");
			Map<String, String> itemMap = table.get(itemId);
			for (String k: itemMap.keySet()) {
				String v = itemMap.get(k);
				System.out.print(k + ": " + v + ", ");
			}
			System.out.println();
		}
	}
	
	private static void runCollateExtractedFile() {
		String inputFileName = "/u/s/a/sanjibkd/Downloads/784_IS/7.txt";
		String outputFile1Name = "/u/s/a/sanjibkd/Downloads/784_IS/walmart_extracted.txt";
		String outputFile2Name = "/u/s/a/sanjibkd/Downloads/784_IS/vendor_extracted.txt";
		
		String[] attributeNames = {"BRAND", "MANUFACTURER", "MODEL", "SCREEN SIZE", "COLOR",
				"PACKAGE QUANTITY", "LENGTH", "WIDTH", "HEIGHT", "SIZE", "WEIGHT", "MATERIAL",
				"LAPTOP COMPARTMENT DIMENSIONS", "PRINT COLOR", "PAGE YIELD", "MPN", "UPC"};
		String table1Name = "Walmart";
		String table2Name = "Vendor";
		try {
			collateExtractedFile(inputFileName, outputFile1Name, attributeNames, table1Name);
			//collateExtractedFile(inputFileName, outputFile2Name, attributeNames, table2Name);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
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
		String outputPath = "/u/s/a/sanjibkd/Downloads/6.txt";
		String[] attributesToExclude = {};
		String[] attributesToExtract1 = {"Brand", "Manufacturer"};
		String[] attributesToExtract2 = {"Model", "Screen Size"};
		String[] attributesToExtract3 = {"Color", "Package Quantity"};
		String[] attributesToExtract4 = {"Length", "Width", "Height", "Size"};
		String[] attributesToExtract5 = {"Weight", "Material"};
		String[] attributesToExtract6 = {"Laptop Compartment Dimensions", "Print Color", "Page Yield"};
		String[] attributesToExtract7 = {"MPN", "UPC"};
		//prepareSampleForStudents(outputPath, attributesToExclude, attributesToExtract6);
		//		String inputFileName = "/media/My Book/sanjib/walmart_catalog/electronics/elec.txt";
		//		String outputFileName = "/u/s/a/sanjibkd/Downloads/material_dictionary.txt";
		//		try {
		//			createDictionary(inputFileName, outputFileName, "material");
		//		}
		//		catch (FileNotFoundException e) {
		//			e.printStackTrace();
		//		}

		String[] inputFileNames = {"/media/My Book/sanjib/walmart_catalog/bmm.txt"};
		//		String[] inputFileNames = {"/u/s/a/sanjibkd/Downloads/elec_catalog_1.txt", "/u/s/a/sanjibkd/Downloads/elec_catalog_2.txt"};
		//		String[] outputFileNames = {"/u/s/a/sanjibkd/Downloads/package_quantity_dictionary.txt",
		//									"/u/s/a/sanjibkd/Downloads/product_type_dictionary.txt",
		//									"/u/s/a/sanjibkd/Downloads/product_category_dictionary.txt",
		//									"/u/s/a/sanjibkd/Downloads/manufacturer_part_number_dictionary.txt",
		//									"/u/s/a/sanjibkd/Downloads/upc_dictionary.txt"};
		//		String[] attributeNames = {"package_quantity", "product_type", "product_category", "manufacturer_part_number", "upc"};

		String[] outputFileNames = {"/u/s/a/sanjibkd/Downloads/bmm_brand_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_manufacturer_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_model_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_color_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_actual_color_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_assembled_product_length_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_assembled_product_width_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_assembled_product_height_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_assembled_product_weight_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_size_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_material_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_package_quantity_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_product_type_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_product_category_dictionary.txt",
				"/u/s/a/sanjibkd/Downloads/bmm_manufacturer_part_number_dictionary.txt",
		"/u/s/a/sanjibkd/Downloads/bmm_upc_dictionary.txt"};
		String[] attributeNames = {"brand",
				"manufacturer",
				"model",
				"color",
				"actual_color",
				"assembled_product_length",
				"assembled_product_width",
				"assembled_product_height",
				"assembled_product_weight",
				"size",
				"material",
				"package_quantity",
				"product_type",
				"product_category",
				"manufacturer_part_number",
		"upc"};

		//		try {
		//			createDictionaries(inputFileNames, outputFileNames, attributeNames);
		//		}
		//		catch (FileNotFoundException e) {
		//			e.printStackTrace();
		//		}
		//runMergeDictionaries();
		//runGetItems();
		//runCreateTablesFromLabeledPairs();
		runCollateExtractedFile();
	}
}