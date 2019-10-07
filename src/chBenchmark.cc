/*
Copyright 2014 Florian Wolf, SAP AG

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "AnalyticalStatistic.h"
#include "DataSource.h"
#include "DbcTools.h"
#include "Log.h"
#include "Queries.h"
#include "Random.h"
#include "Schema.h"
#include "TransactionalStatistic.h"
#include "Transactions.h"
#include "TupleGen.h"
#include "dialect/DialectStrategy.h"

#include <atomic>
#include <err.h>
#include <getopt.h>
#include <pthread.h>
#include <sql.h>
#include <sqltypes.h>
#include <string.h>
#include <unistd.h>

enum class RunState {
    off,
    warmup,
    run,
};

typedef struct {
    pthread_barrier_t* barStart;
    std::atomic<RunState>& runState;
    int threadId;
    SQLHDBC hDBC;
    void* stat;
    int warehouseCount;
} threadParameters;

static void* analyticalThread(void* args) {
    auto prm = (threadParameters*) args;
    auto aStat = (AnalyticalStatistic*) prm->stat;

    bool b;
    int query = 0;
    int q = 0;

    Queries queries;
    if (!queries.prepareStatements(prm->hDBC)) {
        exit(1);
    }

    pthread_barrier_wait(prm->barStart);

    Log::l1() << Log::tm() << "-analytical " << prm->threadId
              << ":  start warmup\n";
    while (prm->runState == RunState::warmup) {
        q = (query % 22) + 1;

        Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPC-H "
                  << q << "\n";
        queries.executeTPCH(q);
        query++;
    }

    Log::l1() << Log::tm() << "-analytical " << prm->threadId
              << ": start test\n";
    while (prm->runState == RunState::run) {
        q = (query % 22) + 1;

        Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPC-H "
                  << q << "\n";
        b = queries.executeTPCH(q);
        aStat->executeTPCHSuccess(q, b);
        query++;
    }

    Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": exit\n";
    return nullptr;
}

static void* transactionalThread(void* args) {
    threadParameters* prm = (threadParameters*) args;
    TransactionalStatistic* tStat = (TransactionalStatistic*) prm->stat;

    Transactions transactions {prm->warehouseCount};
    if (!transactions.prepareStatements(prm->hDBC)) {
        exit(1);
    }

    bool b;

    if (DbcTools::autoCommitOff(prm->hDBC)) {

        pthread_barrier_wait(prm->barStart);

        Log::l1() << Log::tm() << "-transactional " << prm->threadId
                  << ": start warmup\n";
        while (prm->runState == RunState::warmup) {
            auto decision = chRandom::uniformInt(1, 100);

            if (decision <= 44) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": NewOrder\n";
                transactions.executeNewOrder(prm->hDBC);
            }
            else if (decision <= 88) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": Payment\n";
                transactions.executePayment(prm->hDBC);
            }
            else if (decision <= 92) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": OrderStatus\n";
                transactions.executeOrderStatus(prm->hDBC);
            }
            else if (decision <= 96) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": Delivery\n";
                transactions.executeDelivery(prm->hDBC);
            }
            else {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": StockLevel\n";
                transactions.executeStockLevel(prm->hDBC);
            }
            auto sleepTime = chRandom::uniformInt(prm->sleepMin, prm->sleepMax);
            usleep(sleepTime);
        }

        Log::l1() << Log::tm() << "-transactional " << prm->threadId
                  << ": start test\n";
        while (prm->runState == RunState::run) {
            auto decision = chRandom::uniformInt(1, 100);
            if (decision <= 44) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": NewOrder\n";
                b = transactions.executeNewOrder(prm->hDBC);
                tStat->executeTPCCSuccess(1, b);
            }
            else if (decision <= 88) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": Payment\n";
                b = transactions.executePayment(prm->hDBC);
                tStat->executeTPCCSuccess(2, b);
            }
            else if (decision <= 92) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": OrderStatus\n";
                b = transactions.executeOrderStatus(prm->hDBC);
                tStat->executeTPCCSuccess(3, b);
            }
            else if (decision <= 96) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": Delivery\n";
                b = transactions.executeDelivery(prm->hDBC);
                tStat->executeTPCCSuccess(4, b);
            }
            else {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": StockLevel\n";
                b = transactions.executeStockLevel(prm->hDBC);
                tStat->executeTPCCSuccess(5, b);
            }
        }
    }

    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": exit\n";
    return nullptr;
}

static int parseInt(const char* context, const char* v) {
    try {
        return std::stoi(optarg);
    } catch (const std::exception&) {
        errx(1, "unable to parse integer %s for %s\n", v, context);
    }
}

static void usage() {
    fprintf(stderr, "usage: chBenchmark [--warehouses N] [--out-dir PATH] gen\n"
                    "   or: chBenchmark [options] run\n");
}

static int detectWarehouses(SQLHSTMT& hStmt, int* countOut) {
    *countOut = 0;

    DbcTools::executeServiceStatement(
        hStmt, DialectStrategy::getInstance()->getSelectCountWarehouse());

    int temp = 0;
    SQLLEN nIdicator = 0;
    SQLCHAR buf[1024] = {0};
    DbcTools::fetch(hStmt, buf, &nIdicator, 1, temp);
    *countOut = temp;
    if (*countOut == 0) {
        Log::l2() << Log::tm() << "-detecting warehouses failed\n";
        return 1;
    }
    Log::l1() << Log::tm() << "-detected warehouses: " << *countOut << "\n";

    return 0;
}

static int run(int argc, char* argv[]) {
    static struct option longOpts[] = {
        {"dsn", required_argument, nullptr, 'd'},
        {"username", required_argument, nullptr, 'u'},
        {"password", required_argument, nullptr, 'p'},
        {"analytic-threads", required_argument, nullptr, 'a'},
        {"transactional-threads", required_argument, nullptr, 't'},
        {"warmup-seconds", required_argument, nullptr, 'w'},
        {"run-seconds", required_argument, nullptr, 'r'},
        {"gen-dir", required_argument, nullptr, 'g'},
        {"log-file", required_argument, nullptr, 'l'},
        {nullptr, 0, nullptr, 0}};

    int c;
    const char* dsn = nullptr;
    const char* username = nullptr;
    const char* password = nullptr;
    int analyticThreads = 5;
    int transactionalThreads = 10;
    int warmupSeconds = 0;
    int runSeconds = 10;
    std::string genDir = "gen";
    const char* logFile = nullptr;
    while ((c = getopt_long(argc, argv, "d:u:p:a:t:w:r:g:o:", longOpts,
                            nullptr)) != -1) {
        switch (c) {
        case 'd':
            dsn = optarg;
            break;
        case 'u':
            username = optarg;
            break;
        case 'p':
            password = optarg;
            break;
        case 'a':
            analyticThreads = parseInt("analytic threads", optarg);
            break;
        case 't':
            transactionalThreads = parseInt("transactional threads", optarg);
            break;
        case 'w':
            warmupSeconds = parseInt("warmup seconds", optarg);
            break;
        case 'r':
            runSeconds = parseInt("run seconds", optarg);
            break;
        case 'g':
            genDir = optarg;
            break;
        case 'l':
            logFile = optarg;
            break;
        default:
            return 1;
        }
    }
    argc -= optind;
    argv += optind;

    if (!dsn)
        errx(1, "data source name (DSN) must be specified");
    if (analyticThreads < 0)
        errx(1, "analytic threads cannot be negative");
    if (transactionalThreads < 0)
        errx(1, "transactional threads cannot be negative");
    if (analyticThreads == 0 && transactionalThreads == 0)
        errx(
            1,
            "at least one analytic or transactional thread must be configured");
    if (warmupSeconds < 0)
        errx(1, "warmup seconds cannot be negative");
    if (runSeconds < 0)
        errx(1, "run seconds cannot be negative");

    if (logFile)
        Log::open(logFile);

    // Initialization
    Log::l2() << Log::tm() << "Databasesystem:\n-initializing\n";

    // connect to DBS
    SQLHENV hEnv = nullptr;
    DbcTools::setEnv(hEnv);
    SQLHDBC hDBC = nullptr;
    if (!DbcTools::connect(hEnv, hDBC, dsn, username, password)) {
        return 1;
    }

    // create a statement handle for initializing DBS
    SQLHSTMT hStmt = 0;
    SQLAllocHandle(SQL_HANDLE_STMT, hDBC, &hStmt);

    // create database schema
    Log::l2() << Log::tm() << "Schema creation:\n";
    if (!Schema::createSchema(hStmt)) {
        return 1;
    }

    // import initial database from csv files
    Log::l2() << Log::tm() << "CSV import:\n";
    if (!Schema::importCSV(hStmt, genDir)) {
        return 1;
    }

    // detect warehouse count of loaded initial database
    int warehouseCount;
    if (detectWarehouses(hStmt, &warehouseCount))
        return 1;

    // perform a check to ensure that initial database was imported
    // correctly
    if (!Schema::check(hStmt)) {
        return 1;
    }

    // fire additional preparation statements
    Log::l2() << Log::tm() << "Additional Preparation:\n";
    if (!Schema::additionalPreparation(hStmt)) {
        return 1;
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    SQLDisconnect(hDBC);
    SQLFreeHandle(SQL_HANDLE_DBC, hDBC);

    DataSource::initialize(warehouseCount);

    std::atomic<RunState> runState = RunState::off;
    unsigned int count = analyticThreads + transactionalThreads + 1;
    pthread_barrier_t barStart;
    pthread_barrier_init(&barStart, nullptr, count);

    // start analytical threads and create a statistic object for each
    // thread
    AnalyticalStatistic* aStat[analyticThreads];
    pthread_t apt[analyticThreads];
    std::vector<threadParameters> aprm;
    aprm.reserve(analyticThreads);
    for (int i = 0; i < analyticThreads; i++) {
        aStat[i] = new AnalyticalStatistic();
        aprm.push_back(
            {&barStart, runState, i + 1, 0, (void*) aStat[i], warehouseCount});
        if (!DbcTools::connect(hEnv, aprm[i].hDBC, dsn, username, password)) {
            exit(1);
        }
        pthread_create(&apt[i], nullptr, analyticalThread, &aprm[i]);
    }

    // start transactional threads and create a statistic object for each
    // thread
    TransactionalStatistic* tStat[transactionalThreads];
    pthread_t tpt[transactionalThreads];
    std::vector<threadParameters> tprm;
    tprm.reserve(transactionalThreads);
    for (int i = 0; i < transactionalThreads; i++) {
        tStat[i] = new TransactionalStatistic();
        tprm.push_back(
            {&barStart, runState, i + 1, 0, (void*) tStat[i], warehouseCount});
        if (!DbcTools::connect(hEnv, tprm[i].hDBC, dsn, username, password)) {
            exit(1);
        }
        pthread_create(&tpt[i], nullptr, transactionalThread, &tprm[i]);
    }

    runState = RunState::warmup;
    Log::l2() << Log::tm() << "Wait for threads to initialize:\n";
    pthread_barrier_wait(&barStart);
    Log::l2() << Log::tm() << "-all threads initialized\n";

    // main test execution
    Log::l2() << Log::tm() << "Workload:\n";
    Log::l2() << Log::tm() << "-start warmup\n";
    sleep(warmupSeconds);

    runState = RunState::run;
    Log::l2() << Log::tm() << "-start test\n";
    sleep(runSeconds);

    Log::l2() << Log::tm() << "-stop\n";
    runState = RunState::off;

    // write results to file
    unsigned long long analyticalResults = 0;
    unsigned long long transcationalResults = 0;
    for (int i = 0; i < analyticThreads; i++) {
        aStat[i]->addResult(analyticalResults);
    }
    for (int i = 0; i < transactionalThreads; i++) {
        tStat[i]->addResult(transcationalResults);
    }

    unsigned long long qphh = analyticalResults * 3600 / runSeconds;
    unsigned long long tpmc = transcationalResults * 60 / runSeconds;

    printf("System under test:      %s\n", dsn);
    printf("Warehouses:             %d\n", warehouseCount);
    printf("Analytical threads:     %d\n", analyticThreads);
    printf("Transactional threads:  %d\n", transactionalThreads);
    printf("Warmup seconds:         %d\n", warmupSeconds);
    printf("Run seconds:            %d\n", runSeconds);
    printf("\n");
    printf("OLAP throughput [QphH]: %llu\n", qphh);
    printf("OLTP throughput [tpmC]: %llu\n", tpmc);

    Log::l2() << Log::tm()
              << "Wait for clients to return from database calls:\n";
    for (int i = 0; i < analyticThreads; i++) {
        pthread_join(apt[i], nullptr);
    }
    for (int i = 0; i < transactionalThreads; i++) {
        pthread_join(tpt[i], nullptr);
    }

    Log::l2() << Log::tm() << "-finished\n";

    return 0;
}

static int gen(int argc, char* argv[]) {
    static struct option longOpts[] = {
        {"warehouses", required_argument, nullptr, 'w'},
        {"out-dir", required_argument, nullptr, 'o'},
        {nullptr, 0, nullptr, 0}};

    int c;
    int warehouseCount = 1;
    const char* outDir = "gen";
    while ((c = getopt_long(argc, argv, "w:o:", longOpts, nullptr)) != -1) {
        switch (c) {
        case 'w':
            warehouseCount = parseInt("warehouse count", optarg);
            break;
        case 'o':
            outDir = optarg;
            break;
        default:
            return 1;
        }
    }
    argc -= optind;
    argv += optind;

    if (warehouseCount < 1) {
        errx(1, "warehouse count must be greater than zero");
    }

    DataSource::initialize(warehouseCount);
    TupleGen::openOutputFiles(outDir);

    for (int iId = 1; iId <= 100000; iId++) {
        // Item
        TupleGen::genItem(iId);
    }

    std::string customerTime = "";
    std::string orderTime;
    for (int wId = 1; wId <= warehouseCount; wId++) {
        // Warehouse
        TupleGen::genWarehouse(wId);

        for (int iId = 1; iId <= 100000; iId++) {
            // Stock
            TupleGen::genStock(iId, wId);
        }

        for (int dId = 1; dId <= 10; dId++) {
            // District
            TupleGen::genDistrict(dId, wId);
            int nextOlCount = -1;
            for (int cId = 1; cId <= 3000; cId++) {
                // Customer
                if (customerTime.empty())
                    customerTime = DataSource::getCurrentTimeString();
                TupleGen::genCustomer(cId, dId, wId, customerTime);

                // History
                TupleGen::genHistory(cId, dId, wId);

                // Order
                int oId = DataSource::permute(cId, 1, 3000);
                int olCount;
                if (nextOlCount == -1) {
                    olCount = chRandom::uniformInt(5, 15);
                    nextOlCount = 20 - olCount;
                } else {
                    olCount = nextOlCount;
                    nextOlCount = -1;
                }
                orderTime = DataSource::getCurrentTimeString();
                TupleGen::genOrder(oId, dId, wId, cId, olCount, orderTime);

                for (int olNumber = 1; olNumber <= olCount; olNumber++) {
                    // Orderline
                    TupleGen::genOrderline(oId, dId, wId, olNumber, orderTime);
                }

                // Neworder
                if (oId > 2100) {
                    TupleGen::genNeworder(oId, dId, wId);
                }
            }
        }
    }

    // Region
    for (int rId = 0; rId < 5; rId++) {
        TupleGen::genRegion(rId, DataSource::getRegion(rId));
    }

    // Nation
    for (int i = 0; i < 62; i++) {
        TupleGen::genNation(DataSource::getNation(i));
    }

    // Supplier
    for (int suId = 0; suId < 10000; suId++) {
        TupleGen::genSupplier(suId);
    }

    TupleGen::closeOutputFiles();

    // TODO(benesch): check for write errors.

    return 0;
}

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (argv[i][0] == '-')
            continue;
        else if (strcmp(argv[i], "run") == 0)
            return run(argc, argv);
        else if (strcmp(argv[i], "gen") == 0)
            return gen(argc, argv);
        else if (strcmp(argv[i], "version") == 0) {
            fprintf(stderr, "chBenchmark 0.1.0\n");
            return 0;
        } else
            errx(1, "unknown command: %s\n", argv[i]);
    }
    usage();
    return 0;
}
