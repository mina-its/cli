from distutils import dir_util
import sys
import os
import codecs
import enum
from colorama import Fore, init as color_init
from bson import json_util
from pymongo import MongoClient
from bson import ObjectId
import json
import datetime

root_path = ""
db_address = ""
packages = []


class MigrationStatus(enum.IntEnum):
    pending = 1
    done = 2
    error = 3
    rollback_done = 4
    rollback_error = 5


def important(text):
    print(Fore.CYAN + text + Fore.WHITE)


def todo(text):
    print(Fore.WHITE + "TODO:" + text + Fore.WHITE)


def info(text):
    print(Fore.WHITE + text + Fore.WHITE)


def debug(text):
    print(Fore.LIGHTBLACK_EX + text + Fore.WHITE)


def warn(text):
    print(Fore.YELLOW + text + Fore.WHITE)


def err(text):
    print(Fore.RED + text + Fore.WHITE)


def get_db(db_name):
    try:
        return MongoClient(db_address)[db_name]
    except Exception as ex:
        err("Error connecting to '%s' database. %s" % (db_name, ex))
        sys.exit()


def get_db_bson(db, collection_name):
    collection = db[collection_name]
    docs = []
    for doc in collection.find({}):
        docs.append(doc)
    return docs


def get_db_json(db, collection_name):
    docs_bson = get_db_bson(db, collection_name)
    docs_bson_str = json.dumps(docs_bson, default=json_util.default)
    docs_json = json.loads(docs_bson_str)
    return docs_json


def get_json_file(db_name, folder_name, collection_name):
    path_json = os.path.join(root_path, db_name, ".db", folder_name, collection_name + ".json")
    if not os.path.exists(path_json):
        err("File '%s' not found" % path_json)
        return None

    try:
        with open(path_json, encoding='utf-8') as path_json_file:
            json_file = json.load(path_json_file)
    except UnicodeDecodeError as error:
        err("Error decoding json file '" + path_json + "': " + error.reason)
        return None
    except json.decoder.JSONDecodeError as error:
        err("Error in json file '" + path_json + "': " + error.msg)
        return None

    return json_file


def compare_jsons(collection_name, new, old):
    diffs = []
    for doc in old:
        doc_in_new = next(filter(lambda node_doc: str(node_doc["_id"]) == str(doc["_id"]), new), None)
        if doc_in_new is None:
            diffs.append(
                {
                    "_id": ObjectId(),
                    "op": "d",
                    "doc": doc,
                    "ns": collection_name,
                    "key": {"_id": doc["_id"]}
                }
            )
        else:
            if json.dumps(doc) != json.dumps(doc_in_new):
                diffs.append({
                    "_id": ObjectId(),
                    "op": "u",
                    "doc": doc_in_new,
                    "ns": collection_name,
                    "key": {"_id": doc_in_new["_id"]}
                })

    for doc in new:
        doc_in_old = next(filter(lambda local_doc: str(local_doc["_id"]) == str(doc["_id"]), old), None)
        if doc_in_old is None:
            diffs.append({
                "_id": ObjectId(),
                "op": "i",
                "doc": doc,
                "ns": collection_name,
                "key": {"_id": doc["_id"]}
            })
    return diffs


def log_change(change, package):
    op = change["op"]
    if op == "d":
        op = Fore.YELLOW + op
    elif op == "i":
        op = Fore.BLUE + op
    elif op == "u":
        op = Fore.GREEN + op

    _id = change["doc"]["_id"]["$oid"]
    important("%s-[%s] %s:%s" % (op, package, change["ns"], _id) + Fore.WHITE)
    debug("%s" % (change))


def apply_changes_to_database(package, changes):
    client = MongoClient(db_address)
    db = client[package["name"]]
    todo('Use transaction here!')

    for change in changes:
        log_change(change, package["name"])
        doc = json_to_bson(change["doc"])
        key = json_to_bson(change["key"])
        col = db[change["ns"]]
        try:
            already_doc = col.find_one(key)
            if change["op"] == "u":
                if already_doc:
                    col.replace_one(key, doc)
                else:
                    err('document not found for updating!')
            elif change["op"] == "i":
                if already_doc:
                    warn('document already exist. replacing ...')
                    col.replace_one(key, doc)
                else:
                    col.insert_one(doc)
            elif change["op"] == "d":
                if already_doc:
                    col.delete_one(key)
                else:
                    warn('document is already deleted!')
        except Exception as er:
            err("Exception:" + str(er))

    return None


def bson_to_json(bson):
    json_str = json.dumps(bson, default=json_util.default)
    return json.loads(json_str)


def json_to_bson(js):
    bson_str = json.dumps(js)
    return json.loads(bson_str, object_hook=json_util.object_hook)


def export_db_to_folder(package_name, folder, collections):
    db = get_db(package_name)
    root = os.path.join(root_path, package_name, ".db", folder)
    debug("exporting [" + ",".join(collections) + "] to '%s' ..." % root)

    for collection_name in collections:
        path = os.path.join(root, collection_name + ".json")
        collection = db[collection_name]
        cursor = collection.find({})

        file = codecs.open(path, "w", encoding='utf8')
        file.write('[')
        for document in cursor:
            doc = json.dumps(document, ensure_ascii=False, default=json_util.default, indent=False,
                             separators=(",\r\n", ":"))
            file.write(doc)
            file.write(',\r\n')

        if file.tell() > 1:
            file.seek(-3, os.SEEK_END)
            file.flush()
        file.write('\r\n]')

    debug("done!")
    return


def detect_remote_changes(package):
    db_name = package["name"]
    db = get_db(db_name)
    all_changes = []
    _latest_data = get_db_json(db, "_latest")
    for collection in package["syncCollections"]:
        latest = get_json_file(db_name, "latest", collection)
        _latest_bson = next(filter(lambda doc: doc["name"] == collection, _latest_data), None)
        if not _latest_bson:
            warn("collection data '%s' not found in _latest, assumming current database as _latest." % collection)
            _latest = get_db_json(db, collection)
        else:
            _latest = bson_to_json(_latest_bson)["content"]

        changes = compare_jsons(collection, latest, _latest)
        if len(changes) > 0:
            for change in changes:
                all_changes.append(change)

    return all_changes


def detect_unapplied_update(package):
    db_name = package["name"]
    db = get_db(db_name)
    all_changes = []
    _latest = get_db_json(db, "_latest")
    for collection in package["syncCollections"]:
        latest = get_json_file(db_name, "latest", collection)
        _latest_bson = next(filter(lambda doc: doc["name"] == collection, _latest), None)
        if not _latest_bson:
            debug("collection data '%s' not found in _latest" % collection)
            return []

        changes = compare_jsons(collection, latest, bson_to_json(_latest_bson)["content"])
        if len(changes) > 0:
            for change in changes:
                all_changes.append(change)

    return all_changes


def export_all_remote():
    for package in packages:
        export_latest_to_db(package["name"], package["syncCollections"])


def export_latest_to_db(package_name, sync_collections):
    debug("export_latest_to_db ...")
    db = get_db(package_name)
    db["_latest"].delete_many({})
    for collection in sync_collections:
        latest = get_json_file(package_name, "latest", collection)
        db["_latest"].insert_one({
            "name": collection,
            "content": json_to_bson(latest)
        })

    pass


def init_import_collections(db_name, package):
    info("init_import_collections ...")
    db = get_db(db_name)
    collections = list(set(package["syncCollections"])) + list(set(package["initCollections"]))
    for collection in collections:
        if db[collection].estimated_document_count() > 0:
            info("collection '%s' already exists." % collection)
        else:
            js = get_json_file(db_name, "latest", collection)
            if js and len(js) > 0:
                important("importing '%s' ..." % collection)
                db[collection].insert_many(json_to_bson(js))
                info("done!")


def new_package_create_struture(package_name):
    package_project_path = os.path.join(root_path, package_name)
    if os.path.exists(os.path.join(package_project_path, "main.ts")):
        err("package already has been inited.")
        return

    src = os.path.join(root_path, "cli", "new_package_template")
    important("Start copying template files ...")
    dir_util.copy_tree(src, package_project_path)

    package_json_path = os.path.join(package_project_path, "package.json")
    file = codecs.open(package_json_path, "r")
    content = file.read()
    file.close()
    content = content.replace("PACKAGE_NAME", package_name)
    file = codecs.open(package_json_path, "w", encoding='utf8')
    file.write(content)
    file.flush()
    file.close()
    info("done!")


def new_package_create_database(package_name, sync_collections):
    db = get_db(package_name)
    if db["menus"].estimated_document_count() > 0:
        err('Database already exists!')
        sys.exit()

    sys.stdout.write("Please enter the root user email: ")
    sys.stdout.flush()
    user_email = input()

    sys.stdout.write("Please enter the root user password: ")
    sys.stdout.flush()
    user_password = input()

    important("creating database ...")
    role_root_id = ObjectId()

    info("create root role ...")
    db["roles"].insert_one({"_id": role_root_id, "name": "root", "title": "root"})

    info("create admin user ...")
    db["users"].insert_one(
        {"_id": ObjectId(), "name": "admin", "title": "admin", "email": user_email, "password": user_password,
         "roles": [role_root_id]})

    info("create views ...")
    db["views"].insert_one({"_id": ObjectId(), "name": "home", "title": "home", "elems": [{
        "type": 1,
        "_id": ObjectId(),
        "article": {
            "content": "Welcome!"
        }
    }]})

    functions_id = ObjectId()
    objects_id = ObjectId()
    users_id = ObjectId()
    roles_id = ObjectId()
    dic_id = ObjectId()
    views_id = ObjectId()
    enums_id = ObjectId()
    menus_id = ObjectId()

    info("create objects ...")
    access = {"items": [{"role": role_root_id, "permission": 8, "_id": ObjectId()}]}
    db["objects"].insert_many([
        {"_id": functions_id, "name": "functions", "title": "Functions", "source": 1, "isList": True, "access": access,
         "reference": ObjectId("54a51cefd3ca1b12e0e9374d")},
        {"_id": objects_id, "name": "objects", "title": "Objects", "source": 1, "isList": True, "access": access,
         "reference": ObjectId("54a3c8d630c3a4889881b336")},
        {"_id": users_id, "name": "users", "title": "Users", "source": 1, "isList": True, "access": access,
         "reference": ObjectId("54985ac730c392589b16d3c3")},
        {"_id": roles_id, "name": "roles", "title": "Roles", "source": 1, "isList": True, "access": access,
         "reference": ObjectId("54985d4830c392589b16d3c5")},
        {"_id": dic_id, "name": "dictionary", "title": "Dictionary", "source": 1, "isList": True, "access": access,
         "reference": ObjectId("549fcd25d3ca1b10fc490fcc")},
        {"_id": views_id, "name": "views", "title": "Views", "source": 1, "isList": True, "access": access,
         "reference": ObjectId("54993f3430c33d6e6e9dd0d4")},
        {"_id": enums_id, "name": "enums", "title": "Enums", "source": 1, "isList": True, "access": access,
         "reference": ObjectId("5daa0b11e1635a1e6862baac")},
        {"_id": menus_id, "name": "menus", "title": "Menus", "source": 1, "isList": True, "access": access,
         "reference": ObjectId("58e61fb7f2ace02e40cf2c94")}
    ])

    info("create menus ...")
    db["menus"].insert_one({"name": "main", "items": [
        {"_id": ObjectId(), "ref": "home"},
        {"_id": ObjectId(), "title": "Admin", "items": [
            {"_id": ObjectId(), "entity": functions_id},
            {"_id": ObjectId(), "entity": objects_id},
            {"_id": ObjectId(), "entity": users_id},
            {"_id": ObjectId(), "entity": roles_id},
            {"_id": ObjectId(), "entity": dic_id},
            {"_id": ObjectId(), "entity": views_id},
            {"_id": ObjectId(), "entity": enums_id},
            {"_id": ObjectId(), "entity": menus_id}
        ]}
    ]})

    info("create configs ...")
    db["configs"].insert_one(
        {
            "_id": ObjectId(),
            "apps": [{
                "_id": ObjectId(),
                "title": package_name,
                "defaultTemplate": "sys.default",
                "defaultLocale": 1033,
                "home": "home",
                "locales": [1033]
            }]
        })

    db = get_db("sys")
    config_sys = db["configSys"].find_one()
    if not next(filter(lambda p: p["name"] == package_name, config_sys["packages"]), None):
        info("update sys.configSys ...")
        config_sys["packages"].append({
            "_id": ObjectId(),
            "name": package_name,
            "enabled": True,
            "syncCollections": sync_collections,
            "initCollections": ["configs", "users"]
        })
        db["configSys"].replace_one({"_id": config_sys["_id"]}, config_sys)

    important("Add done!")


def init():
    important("#init ...")

    path = os.path.join(os.getcwd(), "package.json")
    if not os.path.exists(path):
        err("File 'package.json' not found")
        return None

    with open(path, encoding='utf-8') as json_file:
        package_file = json.load(json_file)

    sync_collections = ["dictionary", "enums", "functions", "menus", "objects", "roles", "views"]
    package_name = package_file["name"]
    new_package_create_struture(package_name)
    new_package_create_database(package_name, sync_collections)
    export_db_to_folder(package_name, 'latest', sync_collections)
    export_latest_to_db(package_name, sync_collections)

    package_project_path = os.path.join(root_path, package_name)
    os.system("npm i \"" + package_project_path + "\"")
    important("#new_package successfully done!")


def post_update():
    important("#post_update ....")
    for package in packages:
        changes = detect_remote_changes(package)
        if len(changes) > 0:
            if not apply_changes_to_database(package, changes):
                export_latest_to_db(package["name"], package["syncCollections"])
        else:
            info("[%s] no change!" % package["name"])
    important("#post_update successfully done!")


def pre_commit():
    important("#pre_commit ...")
    for package in packages:
        changes = detect_unapplied_update(package)
        if len(changes) > 0:
            err("WARNING !!! some changes in [%s] 'latest' folder is detected. Please post_update first!" % (
                package["name"]))
            for change in changes:
                log_change(change, package["name"])
            err("Stopping, Please check the errors!")
            sys.exit()
        else:
            debug("[%s] compare 'latest' to db. no unapplied update detected!" % (package["name"]))
            export_db_to_folder(package["name"], 'latest', package["syncCollections"])
            changes = detect_remote_changes(package)
            if len(changes) > 0:
                for change in changes:
                    log_change(change, package["name"])
            else:
                info("[%s] no change!" % (package["name"]))
            export_latest_to_db(package["name"], package["syncCollections"])
    important("#pre_commit successfully done!")


def refresh():
    important("#refresh ...")

    if not os.path.exists("package.json"):
        err("File 'package.json' not found")
        return None

    with open("package.json", encoding='utf-8') as json_file:
        package_file = json.load(json_file)

    db_name = package_file["name"]
    package = next(filter(lambda p: p["name"] == db_name, packages), None)
    if not package:
        err("Package '%s' not found! " % db_name)
        sys.exit()

    info("refreshing package '%s' ..." % db_name)
    init_import_collections(db_name, package)
    export_latest_to_db(package["name"], package["syncCollections"])
    important("#refres successfully done!")


def print_usage():
    info("Usage: mina <command> [options]")
    debug("")
    info("Commands:")
    debug("  commit     Pre commits the local database changes")
    debug("  update     Post updates the remote database changes")
    debug("  init       Initiates a mina package")


def main():
    color_init()
    important("Mina CLI")

    global db_address
    try:
        db_address = os.environ['DB_ADDRESS']
    except KeyError:
        err("Environment variable 'DB_ADDRESS' must be set!")
        sys.exit()

    global packages
    try:
        db = get_db('sys')
        config = db["systemConfig"].find_one({})
        packages = config["packages"]
    except:
        err("Error reading collection 'systemConfig' from database 'sys'.")
        sys.exit()

    global root_path
    root_path = os.path.normpath(os.path.join(os.path.dirname(sys.argv[0]), '../'))

    if len(sys.argv) < 2:
        print_usage()
        sys.exit()

    command = sys.argv[1]
    # debug("root_path: " + root_path)
    try:
        if command == "commit":
            pre_commit()
        elif command == "update":
            post_update()
        else:
            func = globals()[command]
            func()
    except KeyError:
        err("Invalid command: '%s' " % command)


main()
