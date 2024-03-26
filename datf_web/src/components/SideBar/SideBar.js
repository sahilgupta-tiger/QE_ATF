import { useNavigate } from "react-router-dom";
import TopBar from "../TopBar/TopBar";
import * as React from 'react';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Toolbar from '@mui/material/Toolbar';
import "./sidebar.css";
import { AppBar, Box, Drawer } from "@mui/material";



const MENU_ITEMS = [
    {
        id: 1,
        name: "Overview",
        icon: 'Group 1413.svg',
        url: '/',
        icon2: 'Group 1397.svg'
    },
    {
        id: 2,
        name: "Execution History",
        icon: 'Group 1398.svg',
        url: '/execution_history',
        icon2: 'Group 1410.svg'
    },
    {
        id: 3,
        name: "Connection History",
        icon: 'Group 1399.svg',
        url: '/connection_history',
        icon2: 'Group 1411.svg'
    },
    {
        id: 4,
        name: "Test Case Setup",
        icon: 'Group 1400.svg',
        url: '/test_case_setup',
        icon2: 'Group 1412.svg'
    },
    {
        id: 5,
        name: "Configuration",
        icon: 'Group 1401.svg',
        url: '/configuration',
        icon2: 'Group 1415.svg'
    },
    {
        id: 6,
        name: "Help",
        icon: 'Group 1402.svg',
        url: '/help',
        icon2: 'Group 1414.svg'
    },
]

const drawerWidth = 240;

const SideBar = () => {
    const [styling, setStyling] = React.useState(1);
    const navigate = useNavigate();

    React.useEffect(() => {
        MENU_ITEMS.map((item) => {

            if (window.location.pathname === item.url) {
                setStyling(item.id);
            }
        });

    }, [window.location.pathname]);

    const itemClickHandler = (url, id) => {
        navigate(url);
        setStyling(id);
        // setIsTabChange(!isTabChange);
    };

    return (
        <div className="sidebar">
            <TopBar />
            <Box>
                <Box sx={{ display: 'flex' }} >
                    <AppBar position="fixed"
                        sx={{
                            width: { sm: `calc(100% - ${drawerWidth}px)` },
                            ml: { sm: `${drawerWidth}px` },
                        }}
                    >
                    </AppBar>
                    <Box
                        component="nav"
                        sx={{ width: { sm: drawerWidth }, flexShrink: { sm: 0 } }}
                    >
                        <Drawer
                            variant="permanent"
                            sx={{
                                display: { xs: 'none', sm: 'block' },
                                '& .MuiDrawer-paper': { boxSizing: 'border-box', width: drawerWidth },
                            }}
                            open
                        >
                            <div className="nav-container">
                                <Toolbar className="toolbar" >
                                    QE DATF
                                </Toolbar>
                                <ListItem key={MENU_ITEMS[0].id} sx={{ display: 'block', borderRadius: 10 }}
                                    onClick={() => {
                                        itemClickHandler(MENU_ITEMS[0].url, MENU_ITEMS[0].id);
                                        navigate(MENU_ITEMS[0].url);
                                        setStyling(MENU_ITEMS[0].id);
                                    }}
                                >
                                    <ListItemButton sx={{
                                        minHeight: 48, px: 2.5, borderRadius: 10,
                                        color: styling === MENU_ITEMS[0].id ? '#F7901D' : 'white',
                                        ':hover': {
                                            color: 'grey',
                                        },
                                    }}
                                    >
                                        <ListItemIcon sx={{
                                            minWidth: 0, justifyContent: 'left', pr: '7px',
                                            color: styling === MENU_ITEMS[0].id ? '#F7901D' : 'white',
                                        }} >
                                            {styling === MENU_ITEMS[0].id ? <img src={`/img/${MENU_ITEMS[0].icon2}`} alt='icon' /> :
                                                <img src={`/img/${MENU_ITEMS[0].icon}`} alt='icon' />}

                                        </ListItemIcon>
                                        <ListItemText primary={MENU_ITEMS[0].name}
                                        // sx={{ opacity: open ? 1 : 0 }}
                                        />
                                    </ListItemButton>
                                </ListItem>
                                <ListItem key={MENU_ITEMS[1].id} sx={{ display: 'block', borderRadius: 10 }}
                                    onClick={() => {
                                        itemClickHandler(MENU_ITEMS[1].url, MENU_ITEMS[1].id);
                                        navigate(MENU_ITEMS[1].url);
                                        setStyling(MENU_ITEMS[1].id);
                                    }} >
                                    <ListItemButton sx={{
                                        minHeight: 48, px: 2.5, borderRadius: 10,
                                        color: styling === MENU_ITEMS[1].id ? '#F7901D' : 'white',
                                        ':hover': {
                                            color: 'grey',
                                        },
                                    }} >
                                        <ListItemIcon sx={{
                                            minWidth: 0, justifyContent: 'left', pr: '7px',
                                            color: styling === MENU_ITEMS[1].id ? '#F7901D' : 'white',
                                        }} >
                                            {styling === MENU_ITEMS[1].id ? <img src={`/img/${MENU_ITEMS[1].icon2}`} alt='icon' /> :
                                                <img src={`/img/${MENU_ITEMS[1].icon}`} alt='icon' />}

                                        </ListItemIcon> <ListItemText primary={MENU_ITEMS[1].name} /> </ListItemButton>
                                </ListItem>
                                <ListItem key={MENU_ITEMS[2].id} sx={{ display: 'block', borderRadius: 10 }}
                                    onClick={() => {
                                        itemClickHandler(MENU_ITEMS[2].url, MENU_ITEMS[2].id);
                                        navigate(MENU_ITEMS[2].url);
                                        setStyling(MENU_ITEMS[2].id);
                                    }} >
                                    <ListItemButton sx={{
                                        minHeight: 48, px: 2.5, borderRadius: 10,
                                        color: styling === MENU_ITEMS[2].id ? '#F7901D' : 'white',
                                        ':hover': {
                                            color: 'grey',
                                        },
                                    }} >
                                        <ListItemIcon sx={{
                                            minWidth: 0, justifyContent: 'left', pr: '7px',
                                            color: styling === MENU_ITEMS[2].id ? '#F7901D' : 'white',
                                        }} >
                                            {styling === MENU_ITEMS[2].id ? <img src={`/img/${MENU_ITEMS[2].icon2}`} alt='icon' /> :
                                                <img src={`/img/${MENU_ITEMS[2].icon}`} alt='icon' />}

                                        </ListItemIcon> <ListItemText primary={MENU_ITEMS[2].name} /> </ListItemButton>
                                </ListItem>
                                <ListItem key={MENU_ITEMS[3].id} sx={{ display: 'block', borderRadius: 10 }}
                                    onClick={() => {
                                        itemClickHandler(MENU_ITEMS[3].url, MENU_ITEMS[3].id);
                                        navigate(MENU_ITEMS[3].url);
                                        setStyling(MENU_ITEMS[3].id);
                                    }} >
                                    <ListItemButton sx={{
                                        minHeight: 48, px: 2.5, borderRadius: 10,
                                        color: styling === MENU_ITEMS[3].id ? '#F7901D' : 'white',
                                        ':hover': {
                                            color: 'grey',
                                        },
                                    }} >
                                        <ListItemIcon sx={{
                                            minWidth: 0, justifyContent: 'left', pr: '7px',
                                            color: styling === MENU_ITEMS[3].id ? '#F7901D' : 'white',
                                        }} >
                                            {styling === MENU_ITEMS[3].id ? <img src={`/img/${MENU_ITEMS[3].icon2}`} alt='icon' /> :
                                                <img src={`/img/${MENU_ITEMS[3].icon}`} alt='icon' />}

                                        </ListItemIcon> <ListItemText primary={MENU_ITEMS[3].name} /> </ListItemButton>
                                </ListItem>
                            </div>
                            <div className="nav-container-lower">
                                <ListItem key={MENU_ITEMS[4].id} sx={{ display: 'block', borderRadius: 10 }}
                                    onClick={() => {
                                        itemClickHandler(MENU_ITEMS[4].url, MENU_ITEMS[4].id);
                                        navigate(MENU_ITEMS[4].url);
                                        setStyling(MENU_ITEMS[4].id);
                                    }} >
                                    <ListItemButton sx={{
                                        minHeight: 48, px: 2.5, borderRadius: 10,
                                        color: styling === MENU_ITEMS[4].id ? '#F7901D' : 'white',
                                        ':hover': {
                                            color: 'grey',
                                        },
                                    }} >
                                        <ListItemIcon sx={{
                                            minWidth: 0, justifyContent: 'left', pr: '7px',
                                            color: styling === MENU_ITEMS[4].id ? '#F7901D' : 'white',
                                        }} >
                                            {styling === MENU_ITEMS[4].id ? <img src={`/img/${MENU_ITEMS[4].icon2}`} alt='icon' /> :
                                                <img src={`/img/${MENU_ITEMS[4].icon}`} alt='icon' />}

                                        </ListItemIcon> <ListItemText primary={MENU_ITEMS[4].name} /> </ListItemButton>
                                </ListItem>
                                <ListItem key={MENU_ITEMS[5].id} sx={{ display: 'block', borderRadius: 10 }}
                                    onClick={() => {
                                        itemClickHandler(MENU_ITEMS[5].url, MENU_ITEMS[5].id);
                                        navigate(MENU_ITEMS[5].url);
                                        setStyling(MENU_ITEMS[5].id);
                                    }} >
                                    <ListItemButton sx={{
                                        minHeight: 48, px: 2.5, borderRadius: 10,
                                        color: styling === MENU_ITEMS[5].id ? '#F7901D' : 'white',
                                        ':hover': {
                                            color: 'grey',
                                        },
                                    }} >
                                        <ListItemIcon sx={{
                                            minWidth: 0, justifyContent: 'left', pr: '7px',
                                            color: styling === MENU_ITEMS[5].id ? '#F7901D' : 'white',
                                        }} >
                                            {styling === MENU_ITEMS[5].id ? <img src={`/img/${MENU_ITEMS[5].icon2}`} alt='icon' /> :
                                                <img src={`/img/${MENU_ITEMS[5].icon}`} alt='icon' />}

                                        </ListItemIcon> <ListItemText primary={MENU_ITEMS[5].name} /> </ListItemButton>
                                </ListItem>
                            </div>
                        </Drawer>
                    </Box>
                </Box>
            </Box>
        </div>
    )
}

export default SideBar;