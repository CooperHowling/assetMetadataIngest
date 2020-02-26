package com.ORGANIZATION.aem.core.workflow;

import com.adobe.granite.asset.api.Asset;
import com.adobe.granite.asset.api.AssetManager;
import com.day.cq.commons.jcr.JcrConstants;
import com.day.cq.dam.api.DamConstants;
import com.day.cq.workflow.WorkflowSession;
import com.day.cq.workflow.exec.WorkItem;
import com.day.cq.workflow.exec.WorkflowData;
import com.day.cq.workflow.exec.WorkflowProcess;
import com.day.cq.workflow.metadata.MetaDataMap;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.jcr.resource.api.JcrResourceConstants;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

@Component(service = WorkflowProcess.class, immediate = true, property = {
        Constants.SERVICE_DESCRIPTION + "=Populate assets with CSV metadata file",
        "process.label" + "=CSV to Metadata Process","service.pid"+"=com.ORGANIZATION.aem.core.workflow.CSVtoMetadataProcess"})

public class CSVtoMetadataProcess implements WorkflowProcess {
    private static final Logger log = LoggerFactory.getLogger(CSVtoMetadataProcess.class);

    @Reference
    private ResourceResolverFactory resolverFactory;

    @Override
    public void execute(WorkItem workItem, WorkflowSession workflowSession, MetaDataMap args) {
        log.info("\n \n CSV WORKFLOW START \n \n ");
        WorkflowData workflowData = workItem.getWorkflowData();
        String path = getPath(workflowData);

        // Authentication validation for session
        final Map<String, Object> authInfo = new HashMap<>();
        authInfo.put(JcrResourceConstants.AUTHENTICATION_INFO_SESSION, workflowSession.getSession());
        try {
            ResourceResolver resourceResolver = resolverFactory.getResourceResolver(authInfo);

            // Use BufferedReader to read in CSV file
            BufferedReader reader = getReader(resourceResolver,path);
            // Traverse through CSV and place it in a HashMap
            Map<Integer, List<List<String>>> metadata = readCSVtoMap(reader);

            Node parentNode = getParentNode(path, resourceResolver);

            inputCSVtoAssets(metadata, resourceResolver, parentNode);

        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    private String getPath(WorkflowData workflowData) {
        String path = workflowData.getPayload().toString();
        String[]link = path.split("\\.");
        String[] extension = link[1].split("/");
        return link[0] +"."+ extension[0];

    }

    private Integer getShotNumber(Node child) throws RepositoryException {
        return Integer.valueOf(child.getName().split("_")[1].split("\\.")[0]);
    }

    private Node getParentNode(String path, ResourceResolver resourceResolver) throws RepositoryException {
        Node assetNode = Objects.requireNonNull(resourceResolver.getResource(path)).adaptTo(Node.class);
        return Objects.requireNonNull(assetNode).getParent();
    }

    private BufferedReader getReader(ResourceResolver resourceResolver, String path) throws RepositoryException {
        Node node = Objects.requireNonNull(resourceResolver.getResource(path + "/jcr:content/renditions/original/jcr:content")).adaptTo(Node.class);
        InputStream inStream = Objects.requireNonNull(node).getProperty("jcr:data").getbinary().getStream();
        return new BufferedReader(new InputStreamReader(inStream));
    }

    private ModifiableValueMap getModifiableValueMapForChild(ResourceResolver resourceResolver, Node child) throws RepositoryException {
        AssetManager assetManager = resourceResolver.adaptTo(AssetManager.class);
        Asset asset = Objects.requireNonNull(assetManager).getAsset(child.getPath());
        String metadataPath = String.format("%s/%s/%s", asset.getPath(), JcrConstants.JCR_CONTENT, DamConstants.METADATA_FOLDER);
        Resource metadataResource = resourceResolver.getResource(metadataPath);
        return Objects.requireNonNull(metadataResource).adaptTo(ModifiableValueMap.class);
    }

    //reads through CSV file and maps metadata in HashMap, assuming row #3 is the shotNumber
    private Map<Integer, List<List<String>>> readCSVtoMap(BufferedReader reader) throws IOException {
        Map<Integer, List<List<String>>> metadata = new HashMap<>();
        List<String> propsAndVals;
        String line;
        Integer shotNumber;
        try {
            String[] firstRow = reader.readLine().split(",", -1);
            //replace anything that isnt a number or letter so metadata can be properly placed in schema
            for(int i=0; i<firstRow.length; i++){
                firstRow[i] = firstRow[i].replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", "");
            }
            while ((line = reader.readLine()) != null) {
                List<List<String>> metaValues = new ArrayList<>();
                String[] currentRow = line.split(",", -1);
                shotNumber = Integer.valueOf(currentRow[2].split(" ")[1]);

                //if shot number already has metadata
                if(metadata.containsKey(shotNumber)){
                    //get the metadata for that shot number
                    metaValues = metadata.get(shotNumber);
                    //create a list to store that existing metadata
                    for(int i=0;i<currentRow.length;i++) {
                        //add new to existing
                        propsAndVals = metaValues.get(i);
                        propsAndVals.add(currentRow[i]);
                        //input property and corresponding metadata into index
                        metaValues.add(propsAndVals);
                    }
                }else{
                    //else it is a new shot number

                    for (int i = 0; i < currentRow.length; i++) {
                        List<String> metadataRow = new ArrayList<>();
                        //key should be shotNumber, first array value should be firstRow header, second is the value of the row
                        //if shotnumber has not been found yet, create new line of properties
                        metadataRow.add(firstRow[i]);
                        //todo check if empty value
                        metadataRow.add(currentRow[i]);
                        metaValues.add(metadataRow);
                    }
                }
                metadata.put(shotNumber, metaValues);
            }
        } catch (Exception e) {
            log.info("Finished traversing metadata");
        }
        reader.close();
        return metadata;
    }

    private void inputCSVtoAssets(Map<Integer, List<List<String>>> metadata, ResourceResolver resourceResolver, Node parentNode) throws RepositoryException {
        //loop through assets in the folder
        for (NodeIterator it = parentNode.getNodes(); it.hasNext(); ) {
            Node child = (Node) it.next();
            //big assumption that only the images have underscores, and must have the same format
            Integer shotNumber;
            //  TODO: Is there a better way to check if it is a picture?
            if (child.getName().contains("_")) {
                try {
                    shotNumber = getShotNumber(child);
                } catch (Exception e) {
                    log.info("No shot number found");
                    continue;
                }
                //get metadata from shotNumber
                List<List<String>> metadataProperties = metadata.get(shotNumber);
                log.info("\n SHOTNUMBER PROCESSING: "+ shotNumber +"\n");
                if (metadataProperties != null) {
                    //get map to be able to input metadata
                    ModifiableValueMap childMap = getModifiableValueMapForChild(resourceResolver, child);
                    //put metadata into asset from arrayList
                    inputMetadataIntoChild(metadataProperties, childMap);
                }
            }
        }
        resourceResolver.close();
    }
    private void inputMetadataIntoChild(List<List<String>> metaValues, ModifiableValueMap mvm) {
        try {
            for (List<String> properties : metaValues) {

                //removes duplicates
                HashSet<String> noHeader = new HashSet<>();
                String header = "";
                //get properties - location 0 is the property type, location 1+ is the property value
                // Check for illegal character '/', replace with space if there
                boolean flag = true;
                for(String property : properties){
                    if(flag){
                        header = property.replace("/"," ");
                        flag = false;
                    }else {
                        if(!property.isEmpty()){
                            noHeader.add(property);
                        }
                    }
                }
                if(noHeader.size()>1){
                    String[] propertiesAsArray = noHeader.toArray(new String[0]);
                    mvm.put(header,propertiesAsArray);
                }else{
                    if(noHeader.iterator().hasNext()) {
                        String propertyAsString = noHeader.iterator().next();
                        mvm.put(header, propertyAsString);
                    }
                }

            }
        }catch(Exception e){
            log.info("ERROR: "+e);
        }
    }
}