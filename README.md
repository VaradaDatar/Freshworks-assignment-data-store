# Freshworks-assignment-data-store
**Functions performed**

1.Create()

2.Read()

3.Delete()

4.DisplayAll()

5.ClearAll()

Upon instantiating the class, the datastore is created as 'database.json' in a user-defined address. If no address was provided, it defaults to the current working address.
The program fulfills the following requirements:

**1.Thread-Safety**
The program accommodates multi-threading and provides a thread-safe handling of the files and objects, using threading locks. The thread locks are acquired and released sequentially before and after every operation.

**2.Single client process access to data store**
The use of file locks permits the use of the file opener to only one client proceess at a time.

**3.Limited file size**
The file size is limited to < 1GB. This is done through a class methoud checkfilesize()
