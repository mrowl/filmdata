from filmdata.lib.plugin_manager import PluginManager

manager = PluginManager('filmdata.metric', __file__,
                        ('run', 'schema'))
