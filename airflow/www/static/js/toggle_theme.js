const STORAGE_THEME_KEY = 'darkTheme';
const HTML_THEME_DATASET_KEY = 'data-color-scheme';
const HTML = document.documentElement;
const TOGGLE_BUTTON_ID = 'themeToggleButton';

const getJsonFromStorage = (key)=>JSON.parse(localStorage.getItem(key));
const updateTheme = (isDark)=>{
    localStorage.setItem(STORAGE_THEME_KEY, isDark);
    HTML.setAttribute(HTML_THEME_DATASET_KEY, isDark ? 'dark' : 'light');
}
;
const initTheme = ()=>{
    const isDark = getJsonFromStorage(STORAGE_THEME_KEY) ?? window.matchMedia('(prefers-color-scheme: dark)').matches;
    updateTheme(isDark);
}
;
const toggleTheme = ()=>{
    const isDark = getJsonFromStorage(STORAGE_THEME_KEY);
    updateTheme(!isDark);
}
;
window.addEventListener('click', (e)=>{
    if (e.target.id !== TOGGLE_BUTTON_ID)
        return;
    toggleTheme();
}
);
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e)=>{
    const isDark = e.matches;
    updateTheme(isDark);
}
);
initTheme();
