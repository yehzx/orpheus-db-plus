def connect_table():
    from orpheusplus.mysql_manager import MySQLManager
    from orpheusplus.user_manager import UserManager
    from orpheusplus.version_data import VersionData

    user = UserManager()
    mydb = MySQLManager(**user.info)
    table = VersionData(cnx=mydb)
    return table