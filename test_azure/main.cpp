#include <was/storage_account.h>
#include <was/blob.h>
#include <cpprest/filestream.h>  
#include <cpprest/containerstream.h>
#include <iostream>

int main() {
    std::cout << "hello world!" << std::endl;
    // Define the connection-string with your values.
    const utility::string_t storage_connection_string(U("DefaultEndpointsProtocol=https;AccountName=cornuslog;AccountKey=JR6MptUo878bCO+eYu2SUF07p+QiiDKbAbawCFSnxvwP+q/aGm7MqnpZMNuOQIGQgmhZ+VBPVSxFiePOLX7s8A==;EndpointSuffix=core.windows.net"));
    

    try
    {
        // Retrieve storage account from connection string.
        azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);

        // Create the blob client.
        azure::storage::cloud_blob_client blob_client = storage_account.create_cloud_blob_client();

        // Retrieve a reference to a container.
        azure::storage::cloud_blob_container container = blob_client.get_container_reference(U("my-sample-container"));
        // Create the container if it doesn't already exist.
        // container.create_if_not_exists();

	// Retrieve reference to a blob named "my-blob-1".
        azure::storage::cloud_block_blob blockBlob = container.get_block_blob_reference(U("my-blob-1"));

	// Create or overwrite the "my-blob-1" blob with contents from a local file.
	concurrency::streams::istream input_stream = concurrency::streams::file_stream<uint8_t>::open_istream(U("DataFile.txt")).get();
        blockBlob.upload_from_stream(input_stream);
        input_stream.close().wait();

	// Create or overwrite the "my-blob-2" and "my-blob-3" blobs with contents from text.
        // Retrieve a reference to a blob named "my-blob-2".
        azure::storage::cloud_block_blob blob2 = container.get_block_blob_reference(U("my-blob-2"));
        blob2.upload_text(U("more text"));

	// Retrieve a reference to a blob named "my-blob-3".
        azure::storage::cloud_block_blob blob3 = container.get_block_blob_reference(U("my-directory/my-sub-directory/my-blob-3"));
        blob3.upload_text(U("other text"));
    }
    catch (const std::exception& e)
    {
        std::wcout << U("Error: ") << e.what() << std::endl;
    }
    
    try
    {
        // Retrieve storage account from connection string.
        azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);

        // Create the blob client.
        azure::storage::cloud_blob_client blob_client = storage_account.create_cloud_blob_client();

        // Retrieve a reference to a container.
        azure::storage::cloud_blob_container container = blob_client.get_container_reference(U("my-sample-container"));

	// Retrieve reference to a blob named "my-blob-4".
        azure::storage::cloud_block_blob blob4 = container.get_block_blob_reference(U("my-blob-4"));
        azure::storage::access_condition condition4 = azure::storage::access_condition::generate_if_not_exists_condition();
	azure::storage::blob_request_options options4;
	azure::storage::operation_context context4;
	blob4.upload_text(U("test new overwrite text"), condition4, options4, context4);
    }
    catch (const std::exception& e)
    {
        std::wcout << U("Error: ") << e.what() << std::endl;
    }

    return 1;
}
