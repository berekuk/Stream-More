d = load tfiles/clients2time.log

figure('visible', 'off')
plot(d(:,1)', d(:,2)')
print('tfiles/rr.png')
