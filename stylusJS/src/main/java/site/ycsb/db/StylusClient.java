package site.ycsb.db;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

public class StylusClient extends DB {

    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    Socket clientSocket = null;
    InputStream inputStream;
    OutputStream outputStream;

    public void init() throws DBException {
        try {
            // TODO : Find a way to insert port through cmd
            clientSocket = new Socket("localhost", 8081);
            inputStream = clientSocket.getInputStream();
            outputStream = clientSocket.getOutputStream();

            System.out.println("Connected to socket at port 8081");
        } catch (Exception e) {
            System.out.println("Error in connecting to socket \n" + e);
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            Map<String, Object> jsonObject = createDynamicJson("GET", key, "dum");

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonString = objectMapper.writeValueAsString(jsonObject);

            // Writing data to socket
            outputStream.write(jsonString.getBytes());

            // Read the response from the server
            byte[] buffer = new byte[1024];
            int bytesRead = inputStream.read(buffer);
            String receivedData = new String(buffer, 0, bytesRead);

            decode(receivedData, fields, result);
            return Status.OK;

        } catch (Exception e) {
            System.out.println("Error in read " + e);
        }
        return Status.ERROR;
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        try {
            String val = encode(values);
            Map<String, Object> jsonObject = createDynamicJson("SET", key, val);

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonString = objectMapper.writeValueAsString(jsonObject);

            // Writing data to socket
            outputStream.write(jsonString.getBytes());

            return Status.OK;
        } catch (Exception e) {
            System.out.println("Error in insert " + e);
        }
        return Status.ERROR;
    }

    @Override
    public Status delete(String table, String key) {
        try {
            Map<String, Object> jsonObject = createDynamicJson("DELETE", key, "dum");

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonString = objectMapper.writeValueAsString(jsonObject);

            outputStream.write(jsonString.getBytes());

            return Status.OK;
        } catch (Exception e) {
            System.out.println("Error in Delete : " + e);
        }
        return Status.ERROR;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        throw new UnsupportedOperationException("Scan operation not supported");
    }

    private static Map<String, Object> createDynamicJson(String task, String userKey, String userValue) {
        // Create a dynamic JSON object based on user input
        Map<String, Object> jsonObject = new HashMap<>();
        jsonObject.put("task", task);

        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> dataObject = new HashMap<>();
        Map<String, Object> commandObject = new HashMap<>();
        commandObject.put("key", userKey);

        if (task.equals("SET"))
            commandObject.put("value", userValue);

        dataObject.put("command", commandObject);
        dataList.add(dataObject);

        jsonObject.put("data", dataList);

        return jsonObject;
    }

    /**
     * Decode the object from server into the storable result.
     *
     * @param source the loaded object.
     * @param fields the fields to check.
     * @param dest   the result passed back to the ycsb core.
     */
    private void decode(final String source, final Set<String> fields, final Map<String, ByteIterator> dest) {
        try {
            JsonNode json = JSON_MAPPER.readTree((String) source);
            boolean checkFields = fields != null && !fields.isEmpty();
            for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
                Map.Entry<String, JsonNode> jsonField = jsonFields.next();
                String name = jsonField.getKey();
                if (checkFields && fields.contains(name)) {
                    continue;
                }
                JsonNode jsonValue = jsonField.getValue();
                if (jsonValue != null && !jsonValue.isNull()) {
                    dest.put(name, new StringByteIterator(jsonValue.asText()));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not decode JSON");
        }
    }

    /**
     * Encode the object for storage.
     *
     * @param source the source value.
     * @return the storable string.
     */
    private String encode(final Map<String, ByteIterator> source) {
        Map<String, String> stringMap = StringByteIterator.getStringMap(source);

        ObjectNode node = JSON_MAPPER.createObjectNode();
        for (Map.Entry<String, String> pair : stringMap.entrySet()) {
            node.put(pair.getKey(), pair.getValue());
        }
        JsonFactory jsonFactory = new JsonFactory();
        Writer writer = new StringWriter();
        try {
            JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
            JSON_MAPPER.writeTree(jsonGenerator, node);
        } catch (Exception e) {
            throw new RuntimeException("Could not encode JSON value");
        }
        return writer.toString();
    }

}