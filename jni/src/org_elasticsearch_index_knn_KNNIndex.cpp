#include <iostream>
#include <thread>
#include <chrono>
#include <iostream>
#include <fstream>
#include "org_elasticsearch_index_knn_KNNIndex.h"

#include "init.h"
#include "index.h"
#include "params.h"
#include "rangequery.h"
#include "knnquery.h"
#include "knnqueue.h"
#include "methodfactory.h"
#include "spacefactory.h"
#include "space.h"

using std::cout;
using std::endl;
using std::thread;
using std::vector;
using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::microseconds;

using similarity::initLibrary;
using similarity::AnyParams;
using similarity::Index;
using similarity::MethodFactoryRegistry;
using similarity::SpaceFactoryRegistry;
using similarity::AnyParams;
using similarity::Space;
using similarity::ObjectVector;
using similarity::Object;
using similarity::KNNQuery;
using similarity::KNNQueue;

extern "C"

JNIEXPORT void JNICALL Java_org_elasticsearch_index_knn_KNNIndex_saveIndex(JNIEnv *env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath) {

    initLibrary();

  ofstream myfile;
  myfile.open ("/tmp/example.txt");
  myfile << "Writing this to a file.\n";

    Space<float> *space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());

    ObjectVector dataset;
    int *object_ids = env->GetIntArrayElements(ids, 0);
    for (int i = 0; i < env->GetArrayLength(vectors); i++) {
        jfloatArray vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
        float *vector = env->GetFloatArrayElements(vectorArray, 0);
        int arrlen = env->GetArrayLength(vectorArray);
        cout << "VAMSHI... size of : " << sizeof(vector) << "REsult:" << arrlen;
        myfile << "VAMSHI... size of : ";
        myfile << sizeof(vector);
        myfile << "Hurray : ";
        myfile << arrlen; 
        dataset.push_back(new Object(object_ids[i], -1, arrlen*sizeof(float), vector));
        env->ReleaseFloatArrayElements(vectorArray, vector, 0);
    }
    
    Index<float>* index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", "l2", *space, dataset);

    index->CreateIndex(AnyParams());
    //index->CreateIndex(AnyParams({"M=100", "efConstruction=256"}));
    myfile << "\nVAMSHI....Indexed dataset size: ";
    myfile << index->GetSize();
    myfile.close();
    index->SaveIndex(env->GetStringUTFChars(indexPath, NULL));
}

JNIEXPORT jobjectArray JNICALL Java_org_elasticsearch_index_knn_KNNIndex_queryIndex(JNIEnv* env, jobject indexObject, jfloatArray queryVector, jint k) {
    jclass indexClass = env->GetObjectClass(indexObject);
    jmethodID getIndex = env->GetMethodID(indexClass, "getIndex", "()J");
    jlong indexValue = env->CallLongMethod(indexObject, getIndex);
    Index<float>* index = reinterpret_cast<Index<float>*>(indexValue);
    //cout << "Loaded dataset size: " << index->GetSize() << endl;

    float* vector = env->GetFloatArrayElements(queryVector, 0);
    int arrlen = env->GetArrayLength(queryVector);
    Space<float>* space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
    KNNQuery<float> query (*space, new Object(-1, -1, arrlen*sizeof(float), vector), k);
    env->ReleaseFloatArrayElements(queryVector, vector, 0);

    index->SetQueryTimeParams(AnyParams({"ef=512"}));

    //microseconds start = duration_cast<microseconds>(system_clock::now().time_since_epoch());
    index->Search(&query);
    //microseconds end = duration_cast<microseconds>(system_clock::now().time_since_epoch());
    KNNQueue<float> *result = query.Result()->Clone();
    //cout << "C search time: " << start.count() << " " << end.count() << " " << (end-start).count() << endl;

    //query.Print();

    int resultSize = result->Size();

    jclass resultClass = env->FindClass("org/elasticsearch/index/knn/KNNQueryResult");
    jmethodID allArgs = env->GetMethodID(resultClass, "<init>", "(IF)V");
    jobjectArray results = env->NewObjectArray(resultSize, resultClass, NULL);
    for (int i = 0; i < resultSize; i++) {
        float distance = result->TopDistance();
        long id = result->Pop()->id();
        env->SetObjectArrayElement(results, i, env->NewObject(resultClass, allArgs, id, distance));
    }
    return results;
}

JNIEXPORT void JNICALL Java_org_elasticsearch_index_knn_KNNIndex_init2(JNIEnv* env, jobject indexObject, jobjectArray vectors, jstring indexPath) {

    initLibrary();

    ObjectVector* dataset = new ObjectVector();
    for (int i = 0; i < env->GetArrayLength(vectors); i++) {
        jfloatArray vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
        float *vector = env->GetFloatArrayElements(vectorArray, 0);
        dataset->push_back(new Object(i, -1, sizeof(vector), vector));
        env->ReleaseFloatArrayElements(vectorArray, vector, 0);
    }

    Space<float>* space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
    Index<float>* index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", "l2", *space, *dataset);
    index->LoadIndex(env->GetStringUTFChars(indexPath, NULL));
    //delete dataset;

    jclass indexClass = env->GetObjectClass(indexObject);
    jmethodID setIndex = env->GetMethodID(indexClass, "setIndex", "(J)V");
    env->CallVoidMethod(indexObject, setIndex, (jlong)index);
}

JNIEXPORT void JNICALL Java_org_elasticsearch_index_knn_KNNIndex_init(JNIEnv* env, jobject indexObject, jstring indexPath) {

    initLibrary();
    Space<float>* space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
    ObjectVector* dataset = new ObjectVector();
    Index<float>* index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", "l2", *space, *dataset);
    index->LoadIndex(env->GetStringUTFChars(indexPath, NULL));

    jclass indexClass = env->GetObjectClass(indexObject);
    jmethodID setIndex = env->GetMethodID(indexClass, "setIndex", "(J)V");
    env->CallVoidMethod(indexObject, setIndex, (jlong)index);
}

JNIEXPORT void JNICALL Java_org_elasticsearch_index_knn_KNNIndex_gc(JNIEnv* env, jobject indexObject) {
    jclass indexClass = env->GetObjectClass(indexObject);
    jmethodID getIndex = env->GetMethodID(indexClass, "getIndex", "()J");
    jlong indexValue = env->CallLongMethod(indexObject, getIndex);
    cout << "Index gc value: " << indexValue << endl;
    Index<float>* index = reinterpret_cast<Index<float>*>(indexValue);
    delete index;
}
