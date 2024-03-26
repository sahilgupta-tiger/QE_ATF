import { BrowserRouter, Route, Routes } from "react-router-dom"
import Layout from "../Layout/Layout";
import Overview from "../../Pages/Overview/Overview";
import Extraction from "../../Pages/Extraction/Extraction";
import ConnectionString from "../../Pages/ConnectionString/ConnectString";
import TestCase from "../../Pages/TestCaseSetUp/TestCase";
import Configuration from "../../Pages/Configuration/Configuration";
import SQLSetup from "../../Pages/SQLSetup/SQLSetup";
import MappingConfig from "../../Pages/MappingConfig/MappingConfig";
import NewConnection from "../../Pages/NewConnection/NewConnection";
import NewTestCase from "../../Pages/NewTestCase/NewTestCase";
import DBTableDemo from "../../Pages/TestCaseSetUp/DBTableDemo";
import ExecutionFail from "../../Pages/Extraction/ExecutionFail";
import CreateSql from "../../Pages/SQLSetup/CreateSql";
import ConnectionDetails from "../../Pages/ConnectionString/ConnectionDetails";
import EditTestCase from "../../Pages/TestCaseSetUp/EditTestCase";
import NewMappingCreation from "../../Pages/MappingConfig/NewMappingCreation";
import DemoProtocol from "../../Pages/Extraction/DemoProtocol";

const AppRoutes = () => {
    return (
        <BrowserRouter>
            <Routes>
                <Route element={<Layout />}>
                    <Route path='/' element={<Overview/>} />
                    <Route path='/execution_history' element={<Extraction/>} />
                    <Route path='/connection_history' element={<ConnectionString/>} />
                    <Route path='/test_case_setup' element={<TestCase/>} />
                    <Route path='/configuration' element={<Configuration/>} />
                    <Route path='/sql_setup' element={<SQLSetup/>} />
                    <Route path='/mapping_config' element={<MappingConfig/>} />
                    <Route path='/new_connection' element={<NewConnection/>} />
                    <Route path='/new_test_case' element={<NewTestCase/>} />
                    <Route path='/db_table_demo' element={<DBTableDemo/>} />
                    <Route path='/execution_fail/:id' element={<ExecutionFail/>} />
                    <Route path='/create_sql' element={<CreateSql/>} />
                    <Route path='/connection_details/:id' element={<ConnectionDetails/>} />
                    <Route path='/edit_test_case/:id' element={<EditTestCase/>} />
                    <Route path='/new_mapping_creation' element={<NewMappingCreation/>} />
                    <Route path='/demo_protocol/:id' element={<DemoProtocol/>} />
                   
                </Route>
            </Routes>
        </BrowserRouter>
    )
}

export default AppRoutes;