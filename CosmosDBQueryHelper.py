import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors

class CosmosHelper():

    def __init__(self, endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName, lastvaluesCollName,shiftCollName,telemetrykey):
        self.client = cosmos_client.CosmosClient(url_connection=endpoint, auth={ 'masterKey': primaryKey })
        print ("Initializing Cosmos Helper")
        self.customerCollection = CosmosCollection(customerCollName, self.client)
        self.dataSourceCollection = CosmosCollection(dataSourceCollName, self.client)
        self.dataDestCollection = CosmosCollection(dataDestCollName, self.client)
        self.lastValuesCollection = CosmosCollection(lastvaluesCollName, self.client)
        self.shiftCollection = CosmosCollection(shiftCollName, self.client)

class CosmosCollection():

    def __init__(self, collectionName, client):
        self.client = client
        self.collectionName = collectionName
        self.options = {}
        self.options['enableCrossPartitionQuery'] = True

    def InsertOne(self, data):
        self.client.CreateItem(self.collectionName, data)

    def InsertMany(self, data):
        for item in data:
            self.InsertOne(item)

    def Select(self, query):
        return self.client.QueryItems(self.collectionName, query, self.options)

    def ReadItems(self):
        return self.client.ReadItems(self.collectionName, self.options)

    def SelectOne(self, query):
        try:
            return list(self.Select(query))[0]
        except:
            return None

    def ReplaceOne(self, newDocument):
        self.client.ReplaceItem(newDocument["_self"], newDocument)#, { 'accessCondition' : { 'type': 'IfMatch', 'condition': newDocument['id'] } })

    def ReplaceMany(self, newDocuments):
        for document in newDocuments:
            self.ReplaceOne(document)





