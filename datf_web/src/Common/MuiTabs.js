import { Tab, Tabs } from "@mui/material";
import { Box } from "@mui/system";
import { styled } from '@mui/material/styles';

const AntTabs = styled(Tabs)({
    // borderBottom: '1px solid #e8e8e8',
    '& .MuiTabs-indicator': {
      backgroundColor: '#F7901D',
    },
  });

  const AntTab = styled((props) => <Tab disableRipple {...props} />)(({ theme }) => ({
    textTransform: 'none',
    minWidth: 0,
    [theme.breakpoints.up('sm')]: {
      minWidth: 0,
    },
   
    marginRight: theme.spacing(1),
    color: 'rgba(0, 0, 0, 0.85)',
    font: 'normal normal normal 14px/50px Articulat CF',
    fontWeight: 500,
    '&.Mui-selected': {
      color: '#F7901D',
      fontWeight: 600,
      font: 'normal normal normal 14px/50px Articulat CF',

      
    },
    '&.Mui-focusVisible': {
      backgroundColor: '#F7901D',
    },
  }));

export default function MuiTabs({ tabs, active, setActive }) {
    return (
        <Box sx={{ mb: 1, backgroundColor: '#FFFF', boxShadow: 1 }}>
            <AntTabs
                variant="scrollable"
                scrollButtons="auto"
                value={active}
                onChange={(_e, newValue) => setActive(newValue)}
            >
                {tabs.map((tab, i) => <AntTab key={i} label={tab.label} value={tab.value} />)}
            </AntTabs>
        </Box>
    )
}